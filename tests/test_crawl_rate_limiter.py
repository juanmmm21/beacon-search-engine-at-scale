"""Tests de `CoordinatedRateLimiter`: las dos propiedades que importan de
verdad al distribuir el rate limiting entre workers son (1) que nunca hay más
de `max_concurrent_per_domain` leases activos a la vez para un dominio, sin
importar cuántos workers compitan, y (2) que un lease nunca reclamado
(worker muerto) libera su hueco solo tras `lease_ttl_seconds`. Se verifican
igual para el doble en memoria y para la implementación real contra
`fakeredis`, con la misma batería de tests parametrizada."""

from __future__ import annotations

import asyncio
import time

import pytest
from fakeredis import aioredis as fakeredis_aioredis

from beacon_scale_infra.crawl.rate_limiter import (
    CoordinatedRateLimiter,
    InMemoryCoordinatedRateLimiter,
    RedisCoordinatedRateLimiter,
)

_URL = "https://example.com/a"


def _build_limiter(
    backend: str, *, max_concurrent_per_domain: int, default_min_delay_seconds: float
) -> CoordinatedRateLimiter:
    if backend == "memory":
        return InMemoryCoordinatedRateLimiter(
            max_concurrent_per_domain=max_concurrent_per_domain,
            default_min_delay_seconds=default_min_delay_seconds,
        )
    client = fakeredis_aioredis.FakeRedis(decode_responses=True)
    return RedisCoordinatedRateLimiter(
        client=client,
        max_concurrent_per_domain=max_concurrent_per_domain,
        default_min_delay_seconds=default_min_delay_seconds,
    )


@pytest.fixture(params=["memory", "redis"])
def backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def test_acquire_then_release_allows_a_second_acquire(backend: str) -> None:
    limiter = _build_limiter(backend, max_concurrent_per_domain=1, default_min_delay_seconds=0)
    token = await limiter.acquire(_URL)
    await limiter.release(_URL, token)
    second_token = await asyncio.wait_for(limiter.acquire(_URL), timeout=1.0)
    assert second_token != token


async def test_concurrency_cap_is_never_exceeded(backend: str) -> None:
    max_concurrent = 3
    limiter = _build_limiter(
        backend, max_concurrent_per_domain=max_concurrent, default_min_delay_seconds=0
    )
    active = 0
    peak_active = 0
    lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal active, peak_active
        token = await limiter.acquire(_URL)
        async with lock:
            active += 1
            peak_active = max(peak_active, active)
        await asyncio.sleep(0.02)
        async with lock:
            active -= 1
        await limiter.release(_URL, token)

    await asyncio.wait_for(
        asyncio.gather(*(worker() for _ in range(max_concurrent * 4))), timeout=5.0
    )
    assert peak_active <= max_concurrent


async def test_different_domains_do_not_share_the_concurrency_cap(backend: str) -> None:
    limiter = _build_limiter(backend, max_concurrent_per_domain=1, default_min_delay_seconds=0)
    token_a = await asyncio.wait_for(limiter.acquire("https://a.example.com/"), timeout=1.0)
    token_b = await asyncio.wait_for(limiter.acquire("https://b.example.com/"), timeout=1.0)
    assert token_a != token_b


async def test_min_delay_is_enforced_across_acquisitions(backend: str) -> None:
    delay_seconds = 0.15
    limiter = _build_limiter(
        backend, max_concurrent_per_domain=5, default_min_delay_seconds=delay_seconds
    )
    token = await limiter.acquire(_URL)
    await limiter.release(_URL, token)

    started_at = time.monotonic()
    await asyncio.wait_for(limiter.acquire(_URL), timeout=2.0)
    elapsed = time.monotonic() - started_at

    assert elapsed >= delay_seconds * 0.8


async def test_per_call_min_delay_overrides_the_default(backend: str) -> None:
    limiter = _build_limiter(backend, max_concurrent_per_domain=5, default_min_delay_seconds=10.0)
    token = await limiter.acquire(_URL, min_delay_seconds=0.0)
    await limiter.release(_URL, token)

    # Con el valor por defecto (10s) esto haría timeout; el override a 0
    # debe dejar pasar la segunda adquisición de inmediato.
    await asyncio.wait_for(limiter.acquire(_URL, min_delay_seconds=0.0), timeout=1.0)


async def test_expired_lease_is_reclaimed_without_an_explicit_release(backend: str) -> None:
    """Un worker que muere sin llamar a `release` no debe bloquear el hueco
    para siempre: pasado `lease_ttl_seconds`, la siguiente adquisición lo
    recicla."""
    lease_ttl_seconds = 0.1
    if backend == "memory":
        limiter: CoordinatedRateLimiter = InMemoryCoordinatedRateLimiter(
            max_concurrent_per_domain=1,
            default_min_delay_seconds=0,
            lease_ttl_seconds=lease_ttl_seconds,
        )
    else:
        client = fakeredis_aioredis.FakeRedis(decode_responses=True)
        limiter = RedisCoordinatedRateLimiter(
            client=client,
            max_concurrent_per_domain=1,
            default_min_delay_seconds=0,
            lease_ttl_seconds=lease_ttl_seconds,
        )

    await limiter.acquire(_URL)  # nunca liberado: simula un worker muerto
    await asyncio.sleep(lease_ttl_seconds * 1.5)
    await asyncio.wait_for(limiter.acquire(_URL), timeout=2.0)
