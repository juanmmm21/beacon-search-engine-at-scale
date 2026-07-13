"""Configuración de la API de la consola (fase 6): prefijos de los artefactos
en el almacenamiento de objetos de fase 0 y parámetros de orquestación de la
búsqueda. Todo configurable por variable de entorno (`BEACON_CONSOLE_*` para
lo propio de esta fase; las credenciales de backend reutilizan las variables
`BEACON_S3_*`/`BEACON_CONSUL_BASE_URL`/`BEACON_REDIS_URL` que el resto de
fases ya usan, ver `console/app.py`).

A diferencia del `AppConfig` de `beacon-search-console` (rutas locales a un
`data/` producido por su bootstrap de proceso único), aquí no hay ninguna
ruta local de entrada: todos los artefactos viven en el `ObjectStorage`
compartido y se descargan al arrancar cada réplica (ver
`console/artifacts.py` y `ARCHITECTURE.md`, fase 6, sobre qué estado se
comparte y qué estado se reconstruye por réplica).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ConsoleAppConfig:
    """Inmutable: se construye una vez al arrancar la réplica (`from_env`) y
    se comparte con toda la app vía `dependencies.ConsoleAppState`."""

    bucket: str = "beacon-scale-dev"
    index_prefix: str = "search-index"
    """Prefijo del índice global *sin comprimir* de fase 3: lo necesitan el
    vocabulario de `query-parser-autocomplete` (spellcheck + autocomplete),
    el lector de features del reranker y el panel de estadísticas."""

    corpus_catalog_object_key: str = "search-index/corpus_catalog.json"
    """Catálogo `doc_id -> fichero de parte de fase 2` (ver
    `index/corpus_catalog.py`), la pieza que sustituye al `documents.jsonl`
    único que `beacon-search-console` carga entero en memoria."""

    pagerank_prefix: str = "pagerank-scores"
    ltr_model_prefix: str = "ltr-model"
    shard_index_prefix: str = "shard-index"
    """Solo para leer `cluster_manifest.json` (nº de shards esperados) y el
    marcador de versión publicado por `shard-index` -- los datos de cada
    shard los descargan sus réplicas, nunca la API."""

    service_name: str = "beacon-scale-shard"
    shard_timeout_seconds: float = 2.0

    candidate_overfetch_multiplier: int = 3
    """Mismo criterio que `beacon-search-console`: sobre-pedir `limit * N`
    candidatos ya fusionados antes de invocar el reranker, para que el LTR
    tenga margen real de reordenación."""

    cache_ttl_seconds: float = 300.0
    """TTL de cada resultado cacheado. La invalidación ante un índice nuevo
    NO depende de este TTL (la versión de índice namespacea las claves, ver
    `console/cache.py`): el TTL solo limpia entradas huérfanas de versiones
    ya retiradas."""

    snippet_parts_cache_max: int = 32
    """Ficheros de parte de fase 2 retenidos en memoria de proceso para
    resolución de snippets (LRU acotado, ver `console/snippets.py`)."""

    artifacts_dir: Path | None = None
    """Directorio local donde descargar los artefactos al arrancar; `None`
    crea uno temporal que se limpia al cerrar la réplica."""

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket no puede estar vacío")
        if not self.index_prefix:
            raise ValueError("index_prefix no puede estar vacío")
        if not self.corpus_catalog_object_key:
            raise ValueError("corpus_catalog_object_key no puede estar vacío")
        if not self.pagerank_prefix:
            raise ValueError("pagerank_prefix no puede estar vacío")
        if not self.ltr_model_prefix:
            raise ValueError("ltr_model_prefix no puede estar vacío")
        if not self.shard_index_prefix:
            raise ValueError("shard_index_prefix no puede estar vacío")
        if not self.service_name:
            raise ValueError("service_name no puede estar vacío")
        if self.shard_timeout_seconds <= 0:
            raise ValueError(
                f"shard_timeout_seconds debe ser positivo, recibido {self.shard_timeout_seconds}"
            )
        if self.candidate_overfetch_multiplier < 1:
            raise ValueError(
                "candidate_overfetch_multiplier debe ser >= 1, "
                f"recibido {self.candidate_overfetch_multiplier}"
            )
        if self.cache_ttl_seconds <= 0:
            raise ValueError(
                f"cache_ttl_seconds debe ser positivo, recibido {self.cache_ttl_seconds}"
            )
        if self.snippet_parts_cache_max <= 0:
            raise ValueError(
                "snippet_parts_cache_max debe ser positivo, "
                f"recibido {self.snippet_parts_cache_max}"
            )

    @classmethod
    def from_env(cls) -> ConsoleAppConfig:
        artifacts_dir_env = os.environ.get("BEACON_CONSOLE_ARTIFACTS_DIR", "")
        return cls(
            bucket=os.environ.get("BEACON_CONSOLE_BUCKET", "beacon-scale-dev"),
            index_prefix=os.environ.get("BEACON_CONSOLE_INDEX_PREFIX", "search-index"),
            corpus_catalog_object_key=os.environ.get(
                "BEACON_CONSOLE_CORPUS_CATALOG_KEY", "search-index/corpus_catalog.json"
            ),
            pagerank_prefix=os.environ.get("BEACON_CONSOLE_PAGERANK_PREFIX", "pagerank-scores"),
            ltr_model_prefix=os.environ.get("BEACON_CONSOLE_LTR_MODEL_PREFIX", "ltr-model"),
            shard_index_prefix=os.environ.get("BEACON_CONSOLE_SHARD_INDEX_PREFIX", "shard-index"),
            service_name=os.environ.get("BEACON_CONSOLE_SERVICE_NAME", "beacon-scale-shard"),
            shard_timeout_seconds=float(
                os.environ.get("BEACON_CONSOLE_SHARD_TIMEOUT_SECONDS", "2.0")
            ),
            candidate_overfetch_multiplier=int(
                os.environ.get("BEACON_CONSOLE_OVERFETCH_MULTIPLIER", "3")
            ),
            cache_ttl_seconds=float(os.environ.get("BEACON_CONSOLE_CACHE_TTL_SECONDS", "300")),
            snippet_parts_cache_max=int(
                os.environ.get("BEACON_CONSOLE_SNIPPET_PARTS_CACHE_MAX", "32")
            ),
            artifacts_dir=Path(artifacts_dir_env) if artifacts_dir_env else None,
        )
