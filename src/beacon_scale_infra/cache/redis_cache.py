"""Implementación real de `CacheStore` sobre Redis (`SET ... PX` / `GET`).

Reutiliza la *misma instancia* de Redis que ya aloja la cola de mensajes y la
coordinación de crawl de fase 1 (`docker-compose.yml`, servicio `redis`), con
un namespace de claves propio (`beacon:console:cache:*`, ver
`console/cache.py`) -- mismo criterio que fase 1 ya aplicó al deduplicador y
al rate limiter: dos namespaces sobre la instancia existente no justifican un
segundo servicio Redis que operar. La *conexión*, en cambio, es propia del
proceso de la API (creada vía `from_url`): la API de la consola no consume
ningún `MessageQueue`, así que no existe ninguna conexión previa que
compartir en ese proceso (ver `ARCHITECTURE.md`, fase 6).

La expiración la aplica Redis nativamente (`PX`): las réplicas de la API
nunca tienen que barrer claves huérfanas, y una entrada de una versión de
índice ya retirada desaparece sola al vencer su TTL.
"""

from __future__ import annotations

import redis.asyncio as redis_asyncio
from redis.exceptions import RedisError

from beacon_scale_infra.errors import CacheError


class RedisCacheStore:
    def __init__(self, *, client: redis_asyncio.Redis) -> None:
        self._client = client

    @classmethod
    def from_url(cls, url: str) -> RedisCacheStore:
        return cls(client=redis_asyncio.Redis.from_url(url, decode_responses=True))

    async def get(self, key: str) -> str | None:
        try:
            value = await self._client.get(key)
        except RedisError as exc:
            raise CacheError(f"fallo al leer la clave {key!r} de la caché: {exc}") from exc
        return None if value is None else str(value)

    async def set(self, key: str, value: str, *, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            raise CacheError(f"ttl_seconds debe ser positivo, recibido {ttl_seconds}")
        try:
            await self._client.set(key, value, px=int(ttl_seconds * 1000))
        except RedisError as exc:
            raise CacheError(f"fallo al escribir la clave {key!r} en la caché: {exc}") from exc

    async def aclose(self) -> None:
        await self._client.aclose()
