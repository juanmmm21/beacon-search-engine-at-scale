"""Tests de contrato de `ConsulServiceRegistry` contra el servidor Consul
falso de `conftest.py`: verifican que este cliente registra, renueva,
descubre y da de baja instancias vía la API HTTP real de Consul."""

from __future__ import annotations

import pytest

from beacon_scale_infra.errors import ServiceRegistryError
from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.registry.consul import ConsulServiceRegistry


@pytest.fixture
def instance() -> ServiceInstance:
    return ServiceInstance(service_id="shard-0", service_name="shard", host="127.0.0.1", port=9300)


async def test_registered_instance_is_immediately_discoverable(
    fake_consul_base_url: str, instance: ServiceInstance
) -> None:
    registry = ConsulServiceRegistry(base_url=fake_consul_base_url)
    try:
        await registry.register(instance, ttl_seconds=10.0)
        found = await registry.discover("shard")
        assert [i.service_id for i in found] == ["shard-0"]
    finally:
        await registry.aclose()


async def test_discover_only_returns_matching_service_name(
    fake_consul_base_url: str, instance: ServiceInstance
) -> None:
    registry = ConsulServiceRegistry(base_url=fake_consul_base_url)
    try:
        await registry.register(instance)
        await registry.register(
            ServiceInstance(service_id="other-0", service_name="other", host="127.0.0.1", port=9301)
        )
        found = await registry.discover("shard")
        assert [i.service_id for i in found] == ["shard-0"]
    finally:
        await registry.aclose()


async def test_deregister_removes_instance_from_discovery(
    fake_consul_base_url: str, instance: ServiceInstance
) -> None:
    registry = ConsulServiceRegistry(base_url=fake_consul_base_url)
    try:
        await registry.register(instance)
        await registry.deregister("shard-0")
        assert await registry.discover("shard") == []
    finally:
        await registry.aclose()


async def test_heartbeat_on_unknown_service_raises(fake_consul_base_url: str) -> None:
    registry = ConsulServiceRegistry(base_url=fake_consul_base_url)
    try:
        with pytest.raises(ServiceRegistryError):
            await registry.heartbeat("never-registered")
    finally:
        await registry.aclose()


async def test_register_rejects_non_positive_ttl(
    fake_consul_base_url: str, instance: ServiceInstance
) -> None:
    registry = ConsulServiceRegistry(base_url=fake_consul_base_url)
    try:
        with pytest.raises(ServiceRegistryError):
            await registry.register(instance, ttl_seconds=0)
    finally:
        await registry.aclose()
