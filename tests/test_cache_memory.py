"""Tests de `InMemoryCacheStore` (implementación local de `CacheStore`),
directamente y sin mocks -- reloj inyectado para testear expiración de TTL de
forma determinista, igual que `test_registry_local.py` hace con el registro
en memoria."""

from __future__ import annotations

import pytest

from beacon_scale_infra.cache.memory import InMemoryCacheStore


class _ManualClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def test_set_then_get_roundtrip() -> None:
    cache = InMemoryCacheStore()
    await cache.set("clave", "valor", ttl_seconds=60.0)
    assert await cache.get("clave") == "valor"


async def test_get_of_a_missing_key_returns_none() -> None:
    cache = InMemoryCacheStore()
    assert await cache.get("no-existe") is None


async def test_entry_expires_after_its_ttl() -> None:
    clock = _ManualClock()
    cache = InMemoryCacheStore(clock=clock)
    await cache.set("clave", "valor", ttl_seconds=10.0)

    clock.now += 9.9
    assert await cache.get("clave") == "valor"

    clock.now += 0.2
    assert await cache.get("clave") is None
    assert len(cache) == 0


async def test_set_overwrites_value_and_ttl() -> None:
    clock = _ManualClock()
    cache = InMemoryCacheStore(clock=clock)
    await cache.set("clave", "viejo", ttl_seconds=5.0)
    clock.now += 4.0
    await cache.set("clave", "nuevo", ttl_seconds=5.0)

    clock.now += 4.0  # 8s desde el primer set, 4s desde el segundo
    assert await cache.get("clave") == "nuevo"


async def test_eviction_prefers_expired_entries_before_lru() -> None:
    clock = _ManualClock()
    cache = InMemoryCacheStore(max_entries=2, clock=clock)
    await cache.set("expirada", "x", ttl_seconds=1.0)
    clock.now += 5.0
    await cache.set("viva-a", "a", ttl_seconds=60.0)
    await cache.set("viva-b", "b", ttl_seconds=60.0)

    # La expulsión por exceso de tamaño se lleva primero la ya expirada, no
    # una entrada viva.
    assert len(cache) == 2
    assert await cache.get("viva-a") == "a"
    assert await cache.get("viva-b") == "b"


async def test_eviction_drops_the_least_recently_used_entry() -> None:
    cache = InMemoryCacheStore(max_entries=2)
    await cache.set("a", "1", ttl_seconds=60.0)
    await cache.set("b", "2", ttl_seconds=60.0)
    # Tocar "a" la convierte en la más recientemente usada.
    assert await cache.get("a") == "1"

    await cache.set("c", "3", ttl_seconds=60.0)

    assert await cache.get("b") is None
    assert await cache.get("a") == "1"
    assert await cache.get("c") == "3"


async def test_non_positive_ttl_is_rejected() -> None:
    cache = InMemoryCacheStore()
    with pytest.raises(ValueError):
        await cache.set("clave", "valor", ttl_seconds=0.0)


def test_non_positive_max_entries_is_rejected() -> None:
    with pytest.raises(ValueError):
        InMemoryCacheStore(max_entries=0)
