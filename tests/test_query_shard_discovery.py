"""Tests de `resolve_shard_targets`: la resolución "un target por shard_id"
que sustituye la lista fija de `ShardTarget` que
`distributed_index_sharding.coordinator.SearchCoordinator` esperaría
recibir directamente (ver `ARCHITECTURE.md`, fase 5, sección "Réplicas por
shard"). Contra `InMemoryServiceRegistry` directamente, sin red, siguiendo el
mismo criterio que el resto de implementaciones locales del ecosistema (ver
`~/Desarrollo/beacon-search-engine/beacon-search-engine-at-scale/CLAUDE.md`).
"""

from __future__ import annotations

import pytest

from beacon_scale_infra.errors import QueryServingError
from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.query.shard_discovery import resolve_shard_targets
from beacon_scale_infra.registry.local import InMemoryServiceRegistry

_SERVICE_NAME = "beacon-scale-shard"


def _instance(service_id: str, shard_id: int, port: int) -> ServiceInstance:
    return ServiceInstance(
        service_id=service_id,
        service_name=_SERVICE_NAME,
        host="127.0.0.1",
        port=port,
        metadata={"shard_id": str(shard_id)},
    )


async def test_one_target_per_shard_id_sorted_ascending() -> None:
    registry = InMemoryServiceRegistry()
    await registry.register(_instance("shard-1-a", 1, 9301))
    await registry.register(_instance("shard-0-a", 0, 9300))
    await registry.register(_instance("shard-2-a", 2, 9302))

    targets = await resolve_shard_targets(registry, _SERVICE_NAME)

    assert [t.shard_id for t in targets] == [0, 1, 2]
    assert {(t.shard_id, t.host, t.port) for t in targets} == {
        (0, "127.0.0.1", 9300),
        (1, "127.0.0.1", 9301),
        (2, "127.0.0.1", 9302),
    }


async def test_multiple_live_replicas_of_the_same_shard_pick_exactly_one_deterministically() -> (
    None
):
    registry = InMemoryServiceRegistry()
    await registry.register(_instance("shard-0-replica-b", 0, 9301))
    await registry.register(_instance("shard-0-replica-a", 0, 9300))

    targets = await resolve_shard_targets(registry, _SERVICE_NAME)

    # Nunca dos targets para el mismo shard_id (SearchCoordinator no
    # dedupica, ver ARCHITECTURE.md) -- exactamente uno, elegido de forma
    # determinista (menor service_id) entre las réplicas vivas.
    assert len(targets) == 1
    assert targets[0].shard_id == 0
    assert targets[0].port == 9300


async def test_choice_only_changes_when_the_chosen_replica_stops_being_alive() -> None:
    registry = InMemoryServiceRegistry()
    await registry.register(_instance("shard-0-replica-a", 0, 9300))
    await registry.register(_instance("shard-0-replica-b", 0, 9301))

    first = await resolve_shard_targets(registry, _SERVICE_NAME)
    second = await resolve_shard_targets(registry, _SERVICE_NAME)
    assert first == second == (first[0],)
    assert first[0].port == 9300

    # Failover real: al desregistrar la réplica elegida (el equivalente
    # instantáneo de que su TTL expire), la siguiente resolución recalcula y
    # elige la otra réplica viva -- la partición sigue teniendo un target.
    await registry.deregister("shard-0-replica-a")
    after_failover = await resolve_shard_targets(registry, _SERVICE_NAME)
    assert len(after_failover) == 1
    assert after_failover[0].port == 9301


async def test_shard_id_with_no_live_replica_is_simply_absent() -> None:
    registry = InMemoryServiceRegistry()
    await registry.register(_instance("shard-0-a", 0, 9300))

    targets = await resolve_shard_targets(registry, _SERVICE_NAME)

    assert [t.shard_id for t in targets] == [0]


async def test_ignores_instances_of_a_different_service_name() -> None:
    registry = InMemoryServiceRegistry()
    await registry.register(_instance("shard-0-a", 0, 9300))
    await registry.register(
        ServiceInstance(
            service_id="other-service-0",
            service_name="some-other-service",
            host="127.0.0.1",
            port=9999,
            metadata={"shard_id": "0"},
        )
    )

    targets = await resolve_shard_targets(registry, _SERVICE_NAME)

    assert len(targets) == 1
    assert targets[0].port == 9300


async def test_instance_missing_shard_id_metadata_raises() -> None:
    registry = InMemoryServiceRegistry()
    await registry.register(
        ServiceInstance(
            service_id="broken-instance",
            service_name=_SERVICE_NAME,
            host="127.0.0.1",
            port=9300,
        )
    )

    with pytest.raises(QueryServingError):
        await resolve_shard_targets(registry, _SERVICE_NAME)


async def test_instance_with_non_numeric_shard_id_metadata_raises() -> None:
    registry = InMemoryServiceRegistry()
    await registry.register(
        ServiceInstance(
            service_id="broken-instance",
            service_name=_SERVICE_NAME,
            host="127.0.0.1",
            port=9300,
            metadata={"shard_id": "not-a-number"},
        )
    )

    with pytest.raises(QueryServingError):
        await resolve_shard_targets(registry, _SERVICE_NAME)


async def test_no_instances_registered_returns_empty_tuple() -> None:
    registry = InMemoryServiceRegistry()

    targets = await resolve_shard_targets(registry, _SERVICE_NAME)

    assert targets == ()
