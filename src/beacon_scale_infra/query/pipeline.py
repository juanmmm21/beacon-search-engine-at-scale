"""Punto de entrada programático de query serving distribuido contra
infraestructura real: ata `shard_discovery.py` (descubrimiento dinámico de
shards vía el `ServiceRegistry` de fase 0) con
`distributed_index_sharding.coordinator.SearchCoordinator` (fan-out + merge,
sin modificar) -- el equivalente de fase 5 a
`distributed_index_sharding.pipeline.DistributedSearchPipeline`, que en vez
de eso levanta un `LocalShardCluster` de subprocesos y le pasa una lista fija
de `ShardTarget` (ver `ARCHITECTURE.md`, fase 5).

Los shards en sí (particionado, servidor HTTP, fan-out, merge) siguen siendo
exactamente los de `distributed-index-sharding`, sin tocar -- esta clase solo
resuelve *qué* lista de `ShardTarget` pasarle a `SearchCoordinator` en cada
búsqueda, en vez de dejarla fija desde el arranque.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any, Final

import aiohttp
from distributed_index_sharding.coordinator import SearchCoordinator
from distributed_index_sharding.models import FanOutResult
from distributed_index_sharding.query_translation import from_parsed_query_dict, from_raw_text
from distributed_index_sharding.shard_client import HttpShardTransport

from beacon_scale_infra.errors import QueryServingError
from beacon_scale_infra.protocols import ServiceRegistry
from beacon_scale_infra.query.shard_discovery import resolve_shard_targets

_DEFAULT_TIMEOUT_SECONDS: Final[float] = 2.0


class DistributedQueryServingPipeline:
    """Gestor de contexto asíncrono: mantiene una única `aiohttp.ClientSession`
    compartida entre búsquedas (mismo motivo que `HttpShardTransport` reutiliza
    una sesión entre shards dentro de una misma búsqueda -- *connection
    pooling*), pero recalcula la lista de `ShardTarget` en cada llamada a
    `search_text`/`search_parsed_query` en vez de fijarla al construirse: un
    `SearchCoordinator` nuevo y barato (no abre sockets por sí mismo, ver su
    propio `__init__` cuando se le inyecta un `transport`) se construye por
    búsqueda con el resultado ya fresco de `resolve_shard_targets`."""

    def __init__(
        self,
        registry: ServiceRegistry,
        *,
        service_name: str = "beacon-scale-shard",
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._registry = registry
        self._service_name = service_name
        self._timeout_seconds = timeout_seconds
        self._session = aiohttp.ClientSession()
        self._transport = HttpShardTransport(session=self._session)

    async def __aenter__(self) -> DistributedQueryServingPipeline:
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

    async def _current_coordinator(self) -> SearchCoordinator:
        targets = await resolve_shard_targets(self._registry, self._service_name)
        if not targets:
            raise QueryServingError(
                f"ninguna réplica viva registrada para el servicio {self._service_name!r}: "
                "no hay ningún shard al que hacer fan-out"
            )
        return SearchCoordinator(
            targets, transport=self._transport, timeout_seconds=self._timeout_seconds
        )

    async def search_parsed_query(
        self, parsed_query: dict[str, Any], *, top_k: int = 10
    ) -> FanOutResult:
        """Contrato principal: recibe `ParsedQuery.to_json_dict()` de
        `query-parser-autocomplete` (ver `distributed_index_sharding.query_translation`)."""
        request = from_parsed_query_dict(parsed_query, top_k=top_k)
        coordinator = await self._current_coordinator()
        return await coordinator.search(request)

    async def search_text(self, text: str, *, top_k: int = 10) -> FanOutResult:
        """Conveniencia standalone: texto en crudo sin operadores, tokenizado
        internamente (ver `distributed_index_sharding.query_translation.from_raw_text`)."""
        request = from_raw_text(text, top_k=top_k)
        coordinator = await self._current_coordinator()
        return await coordinator.search(request)
