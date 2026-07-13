"""Tests de contrato de `RedisCacheStore` contra `fakeredis` (el doble fiel
del SDK real de Redis, mismo criterio que `test_queue_redis_streams.py`):
verifica que este código llama a la API de Redis correctamente (`SET ... PX`,
`GET`, expiración nativa) y que todo fallo del SDK se envuelve en
`CacheError`, nunca se deja escapar crudo."""

from __future__ import annotations

import asyncio

import pytest
from fakeredis import aioredis as fakeredis_aioredis

from beacon_scale_infra.cache.redis_cache import RedisCacheStore
from beacon_scale_infra.errors import CacheError


def _make_store() -> RedisCacheStore:
    return RedisCacheStore(client=fakeredis_aioredis.FakeRedis(decode_responses=True))


async def test_set_then_get_roundtrip() -> None:
    store = _make_store()
    await store.set("beacon:console:cache:v1:abc:q=1:limit=10", "cuerpo", ttl_seconds=60.0)
    assert await store.get("beacon:console:cache:v1:abc:q=1:limit=10") == "cuerpo"
    await store.aclose()


async def test_get_of_a_missing_key_returns_none() -> None:
    store = _make_store()
    assert await store.get("no-existe") is None
    await store.aclose()


async def test_ttl_is_applied_natively_by_redis() -> None:
    client = fakeredis_aioredis.FakeRedis(decode_responses=True)
    store = RedisCacheStore(client=client)
    await store.set("clave", "valor", ttl_seconds=60.0)

    # La expiración la aplica Redis (PX), no este código: el TTL debe quedar
    # registrado en la propia clave.
    ttl_ms = await client.pttl("clave")
    assert 0 < ttl_ms <= 60_000
    await store.aclose()


async def test_entry_expires_via_redis_ttl() -> None:
    store = _make_store()
    await store.set("efimera", "x", ttl_seconds=0.05)
    await asyncio.sleep(0.1)
    assert await store.get("efimera") is None
    await store.aclose()


async def test_backend_failure_is_wrapped_as_cache_error() -> None:
    # `connected=False` es el mecanismo documentado de fakeredis para simular
    # un Redis inalcanzable: cada operación levanta ConnectionError (subclase
    # de RedisError), exactamente lo que este módulo debe envolver.
    client = fakeredis_aioredis.FakeRedis(decode_responses=True, connected=False)
    store = RedisCacheStore(client=client)

    with pytest.raises(CacheError):
        await store.get("clave")
    with pytest.raises(CacheError):
        await store.set("clave", "valor", ttl_seconds=60.0)
    await store.aclose()


async def test_non_positive_ttl_is_rejected_before_reaching_redis() -> None:
    store = _make_store()
    with pytest.raises(CacheError):
        await store.set("clave", "valor", ttl_seconds=0.0)
    await store.aclose()
