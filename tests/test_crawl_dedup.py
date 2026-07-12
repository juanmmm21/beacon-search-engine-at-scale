"""Tests de `SharedDeduplicator`: la propiedad que importa de verdad es que
`try_claim` es atómico -- de N reclamaciones concurrentes de la misma URL,
exactamente una gana, sin importar cuántas tareas compitan a la vez. Se
verifica igual para el doble en memoria (`asyncio.Lock`) y para la
implementación real contra `fakeredis` (`SADD`), para que ambas
implementaciones del protocolo `SharedDeduplicator` queden cubiertas por la
misma propiedad."""

from __future__ import annotations

import asyncio

import pytest
from fakeredis import aioredis as fakeredis_aioredis

from beacon_scale_infra.crawl.dedup import (
    InMemorySharedDeduplicator,
    RedisSharedDeduplicator,
    SharedDeduplicator,
)


@pytest.fixture(params=["memory", "redis"])
def dedup(request: pytest.FixtureRequest) -> SharedDeduplicator:
    if request.param == "memory":
        return InMemorySharedDeduplicator()
    client = fakeredis_aioredis.FakeRedis(decode_responses=True)
    return RedisSharedDeduplicator(client=client)


async def test_first_claim_of_a_url_succeeds(dedup: SharedDeduplicator) -> None:
    assert await dedup.try_claim("https://example.com/a") is True


async def test_second_claim_of_the_same_url_fails(dedup: SharedDeduplicator) -> None:
    await dedup.try_claim("https://example.com/a")
    assert await dedup.try_claim("https://example.com/a") is False


async def test_claims_are_normalized_like_web_crawler_scheduler_urlnorm(
    dedup: SharedDeduplicator,
) -> None:
    """`https://example.com/a` y `https://EXAMPLE.com/a/` normalizan al mismo
    recurso (ver `web_crawler_scheduler.urlnorm.normalize_url`); reclamar una
    debe bloquear la otra."""
    assert await dedup.try_claim("https://example.com/a") is True
    assert await dedup.try_claim("https://EXAMPLE.com/a/") is False


async def test_different_urls_can_both_be_claimed(dedup: SharedDeduplicator) -> None:
    assert await dedup.try_claim("https://example.com/a") is True
    assert await dedup.try_claim("https://example.com/b") is True


async def test_concurrent_claims_of_the_same_url_have_exactly_one_winner(
    dedup: SharedDeduplicator,
) -> None:
    results = await asyncio.gather(*(dedup.try_claim("https://example.com/a") for _ in range(20)))
    assert sum(results) == 1
