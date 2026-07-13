"""Caché compartida de respuestas de búsqueda, namespaceada por versión de
índice.

La clave incluye la versión de contenido del índice que sirvieron los shards
verificados de esa consulta (`beacon:console:cache:v1:<index_version>:...`):
cuando el índice se reconstruye y los shards reinician con la build nueva, su
versión anunciada cambia, el namespace de claves cambia con ella y ninguna
consulta vuelve a leer una entrada de la build anterior -- invalidación por
construcción, sin barridos `SCAN`+`DEL` ni carreras entre réplicas. Las
entradas huérfanas de la versión retirada expiran solas por TTL (ver
`ARCHITECTURE.md`, fase 6, decisión de invalidación).

Un fallo de la caché (Redis caído, entrada corrupta) degrada a "servir sin
caché", registrado -- nunca convierte una búsqueda válida en un error. Solo
se escriben respuestas no degradadas: una respuesta parcial (shards caídos o
excluidos por versión) es un estado transitorio que no debe sobrevivir en la
caché compartida.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Final

from beacon_search_console.models import SearchResponse

from beacon_scale_infra.console.response_serialization import (
    search_response_from_json_dict,
    search_response_to_json_dict,
)
from beacon_scale_infra.errors import CacheError
from beacon_scale_infra.protocols import CacheStore

logger = logging.getLogger(__name__)

_KEY_PREFIX: Final[str] = "beacon:console:cache:v1"


def search_cache_key(index_version: str, raw_query: str, limit: int) -> str:
    """El texto de la query viaja como sha256, no literal: acota la longitud
    de la clave y evita meter entrada de usuario arbitraria en el keyspace de
    Redis. `limit` forma parte de la identidad de la respuesta (cambia
    cuántos resultados lleva), así que también de la clave."""
    query_digest = hashlib.sha256(raw_query.encode("utf-8")).hexdigest()
    return f"{_KEY_PREFIX}:{index_version}:q={query_digest}:limit={limit}"


class SearchResultCache:
    def __init__(self, store: CacheStore, *, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            raise ValueError(f"ttl_seconds debe ser positivo, recibido {ttl_seconds}")
        self._store = store
        self._ttl_seconds = ttl_seconds

    async def get(self, key: str) -> SearchResponse | None:
        try:
            raw = await self._store.get(key)
        except CacheError as exc:
            logger.warning("caché de resultados ilegible (%s): se sirve sin caché", exc)
            return None
        if raw is None:
            return None
        try:
            data = json.loads(raw)
            return search_response_from_json_dict(data)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            # Una entrada corrupta (o escrita por una versión incompatible
            # del contrato) se ignora y se deja expirar por TTL: la búsqueda
            # se recomputa, nunca se responde basura.
            logger.warning("entrada de caché corrupta en %r (%s): se recomputa", key, exc)
            return None

    async def set(self, key: str, response: SearchResponse) -> None:
        if response.degraded:
            return
        body = json.dumps(search_response_to_json_dict(response), ensure_ascii=False)
        try:
            await self._store.set(key, body, ttl_seconds=self._ttl_seconds)
        except CacheError as exc:
            logger.warning("fallo al escribir en la caché de resultados (%s): se continúa", exc)
