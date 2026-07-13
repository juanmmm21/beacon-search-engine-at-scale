"""Estado compartido de la API de la consola, construido una vez por réplica
en el `lifespan` de `app.py` -- la adaptación multi-réplica del `AppState` de
`beacon-search-console` (ver `ARCHITECTURE.md`, fase 6, para la decisión
pieza a pieza):

*   El `pipeline: DistributedSearchPipeline` original (que arranca N
    subprocesos de shard atados a puertos locales de esta máquina -- lo que
    rompe con más de una réplica de la API) se sustituye por
    `ClusterSearchClient`: descubrimiento del clúster real de fase 5 vía el
    `ServiceRegistry` de fase 0, cero procesos de shard propios.
*   Las estructuras en memoria (vocabulario de spellcheck, tries de
    autocomplete, lectores del reranker, stats) se RECONSTRUYEN por réplica:
    son funciones puras de artefactos inmutables de una build concreta del
    índice (descargados en `console/artifacts.py`), así que N réplicas
    construyen exactamente lo mismo y no pueden divergir mientras sirvan la
    misma versión -- no necesitan estado compartido.
*   Lo que sí es estado compartido entre réplicas -- los resultados de
    búsqueda cacheados -- vive en el `CacheStore` de fase 0 (Redis),
    namespaceado por versión de índice (ver `console/cache.py`), y la tabla
    de snippets deja de cargarse entera en memoria: se resuelve bajo demanda
    contra las particiones de fase 2 (ver `console/snippets.py`).
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

from beacon_search_console.index_stats import GlobalIndexStats
from fastapi import Request
from query_parser_autocomplete import pipeline as query_parser_pipeline
from query_parser_autocomplete.autocomplete import AutocompleteIndex
from query_parser_autocomplete.spellcheck import SpellChecker

from beacon_scale_infra.console.artifacts import download_console_artifacts
from beacon_scale_infra.console.cache import SearchResultCache
from beacon_scale_infra.console.cluster_search import ClusterSearchClient
from beacon_scale_infra.console.config import ConsoleAppConfig
from beacon_scale_infra.console.reranking import PreloadedRerankContext
from beacon_scale_infra.console.snippets import PartitionedSnippetResolver
from beacon_scale_infra.protocols import CacheStore, ObjectStorage, ServiceRegistry


@dataclass(slots=True)
class ConsoleAppState:
    config: ConsoleAppConfig
    index_version: str
    num_shards: int
    last_crawled_at: str | None
    global_stats: GlobalIndexStats
    spell_checker: SpellChecker
    autocomplete_index: AutocompleteIndex
    rerank_context: PreloadedRerankContext
    snippet_resolver: PartitionedSnippetResolver
    cluster: ClusterSearchClient
    cache: SearchResultCache
    _owned_artifacts_dir: tempfile.TemporaryDirectory[str] | None

    @classmethod
    async def build(
        cls,
        config: ConsoleAppConfig,
        *,
        storage: ObjectStorage,
        registry: ServiceRegistry,
        cache_store: CacheStore,
    ) -> ConsoleAppState:
        owned_dir: tempfile.TemporaryDirectory[str] | None = None
        if config.artifacts_dir is None:
            owned_dir = tempfile.TemporaryDirectory(prefix="beacon-console-artifacts-")
            destination_root = Path(owned_dir.name)
        else:
            destination_root = config.artifacts_dir
            destination_root.mkdir(parents=True, exist_ok=True)

        try:
            artifacts = await download_console_artifacts(
                storage, config, destination_root=destination_root
            )
            cluster = ClusterSearchClient(
                registry,
                service_name=config.service_name,
                api_index_version=artifacts.index_version,
                timeout_seconds=config.shard_timeout_seconds,
            )
        except Exception:
            if owned_dir is not None:
                owned_dir.cleanup()
            raise

        try:
            state = cls(
                config=config,
                index_version=artifacts.index_version,
                num_shards=artifacts.num_shards,
                last_crawled_at=artifacts.catalog.last_crawled_at,
                global_stats=GlobalIndexStats.from_index_dir(artifacts.index_dir),
                spell_checker=query_parser_pipeline.build_spell_checker(artifacts.index_dir),
                autocomplete_index=query_parser_pipeline.build_autocomplete_index(
                    artifacts.index_dir
                ),
                rerank_context=PreloadedRerankContext.load(
                    index_dir=artifacts.index_dir,
                    pagerank_dir=artifacts.pagerank_dir,
                    model_dir=artifacts.ltr_model_dir,
                ),
                snippet_resolver=PartitionedSnippetResolver(
                    storage,
                    config.bucket,
                    artifacts.catalog,
                    max_cached_parts=config.snippet_parts_cache_max,
                ),
                cluster=cluster,
                cache=SearchResultCache(cache_store, ttl_seconds=config.cache_ttl_seconds),
                _owned_artifacts_dir=owned_dir,
            )
        except Exception:
            await cluster.close()
            if owned_dir is not None:
                owned_dir.cleanup()
            raise
        return state

    async def close(self) -> None:
        await self.cluster.close()
        if self._owned_artifacts_dir is not None:
            self._owned_artifacts_dir.cleanup()


def get_app_state(request: Request) -> ConsoleAppState:
    state: ConsoleAppState = request.app.state.console_state
    return state
