"""Tests de comportamiento de `InMemoryServiceRegistry`, incluyendo expiración
de TTL -- con un reloj falso inyectado, nunca `time.sleep` real, para que la
suite sea determinista y rápida."""

from __future__ import annotations

import pytest

from beacon_scale_infra.errors import ServiceRegistryError
from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.registry.local import InMemoryServiceRegistry


class _FakeClock:
    def __init__(self) -> None:
        self._now = 0.0

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


@pytest.fixture
def clock() -> _FakeClock:
    return _FakeClock()


@pytest.fixture
def registry(clock: _FakeClock) -> InMemoryServiceRegistry:
    return InMemoryServiceRegistry(clock=clock)


def _instance(service_id: str = "shard-0", service_name: str = "shard") -> ServiceInstance:
    return ServiceInstance(
        service_id=service_id, service_name=service_name, host="127.0.0.1", port=9300
    )


async def test_registered_instance_is_immediately_discoverable(
    registry: InMemoryServiceRegistry,
) -> None:
    await registry.register(_instance())
    assert await registry.discover("shard") == [_instance()]


async def test_discover_only_returns_matching_service_name(
    registry: InMemoryServiceRegistry,
) -> None:
    await registry.register(_instance(service_id="a", service_name="shard"))
    await registry.register(_instance(service_id="b", service_name="other"))
    found = await registry.discover("shard")
    assert [i.service_id for i in found] == ["a"]


async def test_instance_disappears_after_ttl_without_heartbeat(
    registry: InMemoryServiceRegistry, clock: _FakeClock
) -> None:
    await registry.register(_instance(), ttl_seconds=10.0)
    clock.advance(10.1)
    assert await registry.discover("shard") == []


async def test_heartbeat_renews_ttl(registry: InMemoryServiceRegistry, clock: _FakeClock) -> None:
    await registry.register(_instance(), ttl_seconds=10.0)
    clock.advance(8.0)
    await registry.heartbeat("shard-0")
    clock.advance(8.0)
    assert len(await registry.discover("shard")) == 1


async def test_heartbeat_on_unknown_service_raises(registry: InMemoryServiceRegistry) -> None:
    with pytest.raises(ServiceRegistryError):
        await registry.heartbeat("never-registered")


async def test_deregister_is_idempotent(registry: InMemoryServiceRegistry) -> None:
    await registry.register(_instance())
    await registry.deregister("shard-0")
    await registry.deregister("shard-0")  # no debe lanzar
    assert await registry.discover("shard") == []


async def test_register_rejects_non_positive_ttl(registry: InMemoryServiceRegistry) -> None:
    with pytest.raises(ServiceRegistryError):
        await registry.register(_instance(), ttl_seconds=0)
