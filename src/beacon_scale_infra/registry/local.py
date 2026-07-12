"""Registro de servicio en memoria para desarrollo local y tests: cada
instancia guarda la marca de tiempo de su último `heartbeat`; `discover`
filtra las que superaron su TTL en vez de esperar un desregistro explícito --
un shard que muere sin avisar (proceso matado, red caída) no debe seguir
apareciendo como vivo para siempre, la misma garantía que
`ConsulServiceRegistry` obtiene de la TTL health check nativa de Consul.

El reloj es inyectable (`clock`) precisamente para poder testear expiración
de TTL de forma determinista, sin `time.sleep` real en la suite de tests.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from beacon_scale_infra.errors import ServiceRegistryError
from beacon_scale_infra.models import ServiceInstance


@dataclass
class _RegisteredInstance:
    instance: ServiceInstance
    ttl_seconds: float
    last_heartbeat_monotonic: float


class InMemoryServiceRegistry:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._instances: dict[str, _RegisteredInstance] = {}

    async def register(self, instance: ServiceInstance, *, ttl_seconds: float = 30.0) -> None:
        if ttl_seconds <= 0:
            raise ServiceRegistryError(f"ttl_seconds debe ser positivo, recibido {ttl_seconds}")
        self._instances[instance.service_id] = _RegisteredInstance(
            instance=instance,
            ttl_seconds=ttl_seconds,
            last_heartbeat_monotonic=self._clock(),
        )

    async def deregister(self, service_id: str) -> None:
        self._instances.pop(service_id, None)

    async def heartbeat(self, service_id: str) -> None:
        registered = self._instances.get(service_id)
        if registered is None:
            raise ServiceRegistryError(f"servicio no registrado: {service_id!r}")
        registered.last_heartbeat_monotonic = self._clock()

    async def discover(self, service_name: str) -> list[ServiceInstance]:
        now = self._clock()
        alive: list[ServiceInstance] = []
        expired_ids: list[str] = []
        for service_id, registered in self._instances.items():
            if now - registered.last_heartbeat_monotonic > registered.ttl_seconds:
                expired_ids.append(service_id)
                continue
            if registered.instance.service_name == service_name:
                alive.append(registered.instance)
        for service_id in expired_ids:
            self._instances.pop(service_id, None)
        return alive
