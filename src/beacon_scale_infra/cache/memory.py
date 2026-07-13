"""Implementación en memoria de `CacheStore` para desarrollo local y tests:
misma semántica observable que el backend Redis (TTL obligatorio, `get` de
una clave expirada devuelve `None`), sin ninguna dependencia de red.

Acotada por construcción (`max_entries`), nunca de crecimiento ilimitado
(regla de `CLAUDE.md`, sección 2): al superar el límite se expulsa primero
lo ya expirado y después la entrada usada menos recientemente (orden LRU
mantenido con un `dict` ordinario y `move_to_end` implícito vía
borrar-y-reinsertar en cada acceso -- el orden de inserción de `dict` es
determinista, ver la regla de determinismo de las implementaciones locales
en `CLAUDE.md`).

El reloj es inyectable (`clock`), igual que en `InMemoryServiceRegistry`,
para testear expiración de TTL de forma determinista sin `time.sleep`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(slots=True)
class _CacheEntry:
    value: str
    expires_at_monotonic: float


class InMemoryCacheStore:
    def __init__(
        self,
        *,
        max_entries: int = 1024,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_entries <= 0:
            raise ValueError(f"max_entries debe ser positivo, recibido {max_entries}")
        self._max_entries = max_entries
        self._clock = clock
        self._entries: dict[str, _CacheEntry] = {}

    async def get(self, key: str) -> str | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if self._clock() >= entry.expires_at_monotonic:
            del self._entries[key]
            return None
        # Reinsertar renueva la posición LRU: los `dict` de Python preservan
        # orden de inserción, así que la primera clave del dict es siempre la
        # menos recientemente usada.
        del self._entries[key]
        self._entries[key] = entry
        return entry.value

    async def set(self, key: str, value: str, *, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            raise ValueError(f"ttl_seconds debe ser positivo, recibido {ttl_seconds}")
        self._entries.pop(key, None)
        self._entries[key] = _CacheEntry(
            value=value, expires_at_monotonic=self._clock() + ttl_seconds
        )
        self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        if len(self._entries) <= self._max_entries:
            return
        now = self._clock()
        expired_keys = [
            key for key, entry in self._entries.items() if now >= entry.expires_at_monotonic
        ]
        for key in expired_keys:
            del self._entries[key]
        while len(self._entries) > self._max_entries:
            oldest_key = next(iter(self._entries))
            del self._entries[oldest_key]

    def __len__(self) -> int:
        return len(self._entries)
