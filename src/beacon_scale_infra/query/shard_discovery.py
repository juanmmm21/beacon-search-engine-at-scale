"""Resuelve, a partir del `ServiceRegistry` de fase 0, la lista "un target por
shard_id" que `distributed_index_sharding.coordinator.SearchCoordinator`
espera -- sin tocar ese proyecto (ver `AGENTS.md`).

`SearchCoordinator` no dedupica por `shard_id`: si se le pasan dos réplicas
vivas del mismo shard, hace fan-out a ambas y el merge las trataría como dos
shards distintos, duplicando resultados (ver `ARCHITECTURE.md`, fase 5,
sección "Réplicas por shard"). `resolve_shard_targets` es el punto donde se
resuelve ese matiz: agrupa las instancias vivas por `shard_id` y elige
exactamente una por grupo, antes de que el resultado llegue a
`SearchCoordinator`.
"""

from __future__ import annotations

from distributed_index_sharding.models import ShardTarget

from beacon_scale_infra.errors import QueryServingError
from beacon_scale_infra.models import ServiceInstance
from beacon_scale_infra.protocols import ServiceRegistry

SHARD_ID_METADATA_KEY = "shard_id"


def _shard_id_of(instance: ServiceInstance) -> int:
    raw = instance.metadata.get(SHARD_ID_METADATA_KEY)
    if raw is None:
        raise QueryServingError(
            f"instancia {instance.service_id!r} de {instance.service_name!r} no lleva "
            f"metadata {SHARD_ID_METADATA_KEY!r}: no se puede resolver a qué shard sirve"
        )
    try:
        return int(raw)
    except ValueError as exc:
        raise QueryServingError(
            f"metadata {SHARD_ID_METADATA_KEY!r} no numérica en {instance.service_id!r}: {raw!r}"
        ) from exc


async def resolve_shard_targets(
    registry: ServiceRegistry, service_name: str
) -> tuple[ShardTarget, ...]:
    """Descubre las réplicas vivas de `service_name` y devuelve exactamente un
    `ShardTarget` por `shard_id` presente entre ellas, ordenado por `shard_id`.

    Entre varias réplicas vivas del mismo `shard_id`, la elección es
    determinista (menor `service_id` en orden lexicográfico) para que el
    resultado no cambie de réplica en cada llamada mientras todas las
    candidatas sigan vivas -- solo cambia cuando la elegida deja de estar
    entre las vivas que `discover` devuelve (failover real, no alternancia
    cosmética). Un `shard_id` sin ninguna réplica viva en este momento
    simplemente no aparece en el resultado: esa partición del índice queda
    fuera del fan-out hasta que una réplica vuelva a anunciarse, exactamente
    el mismo comportamiento de degradación por timeout/error que
    `distributed_index_sharding.coordinator.SearchCoordinator` ya tolera para
    un shard que no responde.
    """
    instances = await registry.discover(service_name)
    chosen_by_shard: dict[int, ServiceInstance] = {}
    for instance in instances:
        shard_id = _shard_id_of(instance)
        current = chosen_by_shard.get(shard_id)
        if current is None or instance.service_id < current.service_id:
            chosen_by_shard[shard_id] = instance
    return tuple(
        ShardTarget(shard_id=shard_id, host=instance.host, port=instance.port)
        for shard_id, instance in sorted(chosen_by_shard.items())
    )
