"""Implementación real de `ServiceRegistry` contra la API HTTP de Consul
(agente local en modo dev, ver `docker-compose.yml`). Se habla directamente
con la API REST vía `aiohttp` en vez de añadir el cliente `python-consul`
como dependencia: ese paquete lleva años sin mantenimiento activo ni tipado,
y la superficie que necesitamos (registrar, dar de baja, pasar una TTL
check, descubrir instancias sanas) es pequeña y estable -- ver
`ARCHITECTURE.md`, sección "Registro de servicio".

El TTL de vida de una instancia se modela con una health check de tipo TTL
propia de Consul (`Check.TTL`), no con un timestamp que gestionemos
nosotros: si `heartbeat` no se llama a tiempo, Consul marca la check como
"critical" y `GET /v1/health/service/<name>?passing=true` deja de devolver
esa instancia automáticamente -- la misma garantía que
`InMemoryServiceRegistry` implementa a mano para el caso local.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import aiohttp

from beacon_scale_infra.errors import ServiceRegistryError
from beacon_scale_infra.models import ServiceInstance

_MIN_DEREGISTER_AFTER_SECONDS = 60.0


class ConsulServiceRegistry:
    def __init__(self, *, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None

    @staticmethod
    def _check_id(service_id: str) -> str:
        return f"service:{service_id}"

    async def register(self, instance: ServiceInstance, *, ttl_seconds: float = 30.0) -> None:
        if ttl_seconds <= 0:
            raise ServiceRegistryError(f"ttl_seconds debe ser positivo, recibido {ttl_seconds}")
        body: dict[str, Any] = {
            "ID": instance.service_id,
            "Name": instance.service_name,
            "Address": instance.host,
            "Port": instance.port,
            "Meta": dict(instance.metadata),
            "Check": {
                "TTL": f"{ttl_seconds}s",
                "DeregisterCriticalServiceAfter": (
                    f"{max(ttl_seconds * 10, _MIN_DEREGISTER_AFTER_SECONDS)}s"
                ),
            },
        }
        await self._request("PUT", "/v1/agent/service/register", json_body=body)
        # la check TTL nace en estado "critical" hasta el primer pase, así
        # que la instancia no sería descubrible hasta el primer heartbeat
        # externo si no se pasa aquí una vez de forma inmediata.
        await self.heartbeat(instance.service_id)

    async def deregister(self, service_id: str) -> None:
        await self._request("PUT", f"/v1/agent/service/deregister/{service_id}")

    async def heartbeat(self, service_id: str) -> None:
        try:
            await self._request("PUT", f"/v1/agent/check/pass/{self._check_id(service_id)}")
        except ServiceRegistryError as exc:
            raise ServiceRegistryError(f"servicio no registrado en consul: {service_id!r}") from exc

    async def discover(self, service_name: str) -> list[ServiceInstance]:
        data = await self._request(
            "GET", f"/v1/health/service/{service_name}", params={"passing": "true"}
        )
        instances: list[ServiceInstance] = []
        for entry in data or []:
            service = entry["Service"]
            instances.append(
                ServiceInstance(
                    service_id=service["ID"],
                    service_name=service["Service"],
                    host=service["Address"],
                    port=service["Port"],
                    metadata=service.get("Meta") or {},
                )
            )
        return instances

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, Any] | None = None,
        params: Mapping[str, str] | None = None,
    ) -> Any:
        session = await self._get_session()
        url = f"{self._base_url}{path}"
        try:
            async with session.request(method, url, json=json_body, params=params) as response:
                if response.status >= 400:
                    text = await response.text()
                    raise ServiceRegistryError(
                        f"consul respondió {response.status} en {method} {path}: {text}"
                    )
                if response.content_type == "application/json":
                    return await response.json()
                return None
        except aiohttp.ClientError as exc:
            raise ServiceRegistryError(
                f"fallo de red hablando con consul en {method} {path}: {exc}"
            ) from exc

    async def aclose(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
