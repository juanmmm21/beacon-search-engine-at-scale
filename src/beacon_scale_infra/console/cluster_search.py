"""Fan-out de la consola contra el clúster real de fase 5, con verificación
de versión de índice por consulta.

Es la misma composición que `DistributedQueryServingPipeline` (fase 5):
descubrimiento fresco antes de cada búsqueda + un `SearchCoordinator` barato
por consulta sobre una única `aiohttp.ClientSession` compartida. Se
reimplementa aquí (unas pocas líneas de orquestación, nunca fan-out ni merge
propios) porque la consola necesita algo que aquella clase no expone: *qué
versión del índice anuncia la réplica elegida de cada shard*, para decidir
por consulta si el resultado es cacheable y si es coherente con los
artefactos (corpus, reranker, vocabulario) que esta réplica de la API cargó
al arrancar -- ver `ARCHITECTURE.md`, fase 6.

Reglas de coherencia por shard elegido:

*   versión anunciada == la de la API -> entra en el fan-out; cacheable.
*   versión anunciada distinta -> se EXCLUYE del fan-out y se reporta como
    error explícito en `shard_statuses`: mezclar sus `doc_id` con el corpus
    de otra build produciría snippets de documentos equivocados -- el
    resultado obsoleto silencioso que esta fase existe para impedir.
*   sin versión anunciada (réplica arrancada sobre un `shard-index/` sin
    marcador) -> entra en el fan-out (mismo comportamiento que la consola
    original, que no verificaba nada), pero la consulta deja de ser
    cacheable: sin versión no hay namespace de caché seguro.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Any

import aiohttp
from distributed_index_sharding.coordinator import SearchCoordinator
from distributed_index_sharding.models import FanOutResult, ShardTarget
from distributed_index_sharding.query_translation import from_parsed_query_dict
from distributed_index_sharding.shard_client import HttpShardTransport

from beacon_scale_infra.protocols import ServiceRegistry
from beacon_scale_infra.query.shard_discovery import (
    INDEX_VERSION_METADATA_KEY,
    choose_shard_instances,
    shard_id_of,
)


@dataclass(frozen=True, slots=True)
class ClusterView:
    """Foto del clúster tomada justo antes de una búsqueda, ya evaluada
    contra la versión de índice de esta réplica de la API."""

    targets: tuple[ShardTarget, ...]
    """Un target por shard coherente (o de versión desconocida) -- lo que
    entra en el fan-out."""

    version_mismatched_shard_ids: tuple[int, ...]
    """Shards cuya réplica elegida anuncia otra versión de índice: excluidos
    del fan-out, reportados como error explícito."""

    unverified_shard_ids: tuple[int, ...]
    """Shards cuya réplica elegida no anuncia versión: dentro del fan-out,
    pero la consulta no se cachea."""

    @property
    def cacheable(self) -> bool:
        """Solo es seguro cachear cuando cada shard del fan-out está
        verificado contra la versión de la API y ninguno quedó excluido por
        versión: una respuesta parcial o sin verificar no debe sobrevivir en
        la caché compartida."""
        return not self.version_mismatched_shard_ids and not self.unverified_shard_ids


class ClusterSearchClient:
    """Gestor de contexto asíncrono; una única sesión HTTP compartida entre
    búsquedas (connection pooling, mismo criterio que fase 5)."""

    def __init__(
        self,
        registry: ServiceRegistry,
        *,
        service_name: str,
        api_index_version: str,
        timeout_seconds: float,
    ) -> None:
        self._registry = registry
        self._service_name = service_name
        self._api_index_version = api_index_version
        self._timeout_seconds = timeout_seconds
        self._session = aiohttp.ClientSession()
        self._transport = HttpShardTransport(session=self._session)

    async def __aenter__(self) -> ClusterSearchClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        await self._session.close()

    async def snapshot(self) -> ClusterView:
        """Descubre las réplicas vivas ahora mismo (una por `shard_id`, el
        criterio determinista de fase 5) y las clasifica frente a la versión
        de índice de esta réplica de la API."""
        instances = await self._registry.discover(self._service_name)
        chosen = choose_shard_instances(instances)
        targets: list[ShardTarget] = []
        mismatched: list[int] = []
        unverified: list[int] = []
        for instance in chosen:
            shard_id = shard_id_of(instance)
            announced_version = instance.metadata.get(INDEX_VERSION_METADATA_KEY)
            if announced_version is not None and announced_version != self._api_index_version:
                mismatched.append(shard_id)
                continue
            if announced_version is None:
                unverified.append(shard_id)
            targets.append(ShardTarget(shard_id=shard_id, host=instance.host, port=instance.port))
        return ClusterView(
            targets=tuple(targets),
            version_mismatched_shard_ids=tuple(mismatched),
            unverified_shard_ids=tuple(unverified),
        )

    async def search_parsed_query(
        self, view: ClusterView, parsed_query: dict[str, Any], *, top_k: int
    ) -> FanOutResult:
        """Fan-out + merge de `distributed-index-sharding`, sin modificar,
        sobre los targets ya clasificados de `view` (la foto se toma una vez
        por consulta y se reutiliza para el fan-out y para componer los
        estados de shard de la respuesta -- nunca dos descubrimientos que
        podrían divergir dentro de la misma búsqueda)."""
        request = from_parsed_query_dict(parsed_query, top_k=top_k)
        coordinator = SearchCoordinator(
            view.targets, transport=self._transport, timeout_seconds=self._timeout_seconds
        )
        return await coordinator.search(request)
