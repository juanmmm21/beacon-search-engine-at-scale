"""Interfaces del sustrato compartido: almacenamiento de objetos, cola de
mensajes y registro de servicio.

Cada protocolo desacopla la lógica que lo consume (futuras fases de crawl e
indexación distribuidos) de qué backend concreto hay detrás — el mismo
desacoplamiento que `distributed_index_sharding.protocols.ShardTransport` ya
aplica entre el coordinador de fan-out y el transporte físico hacia un shard.
Todas las operaciones son `async`: incluso la implementación local de
filesystem expone una interfaz async para que el código que la consume no
tenga que ramificar entre "modo local" y "modo real" (ver `ARCHITECTURE.md`).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any, Protocol, runtime_checkable

from beacon_scale_infra.models import ObjectMetadata, QueueMessage, ServiceInstance


@runtime_checkable
class ObjectStorage(Protocol):
    """Almacén de objetos (páginas crudas, documentos extraídos, índices ya
    construidos) direccionado por `(bucket, key)`, sin semántica de
    directorios más allá de un `prefix` plano — el mismo modelo que S3/MinIO
    exponen de forma nativa."""

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> ObjectMetadata:
        """Escribe (o sobrescribe) un objeto. Levanta `ObjectStorageError`
        ante cualquier fallo de escritura (disco lleno, red, permisos)."""
        ...

    async def get_object(self, bucket: str, key: str) -> bytes:
        """Levanta `ObjectNotFoundError` si `key` no existe en `bucket`."""
        ...

    async def delete_object(self, bucket: str, key: str) -> None:
        """Idempotente: borrar una clave que ya no existe no es un error."""
        ...

    async def object_exists(self, bucket: str, key: str) -> bool: ...

    def list_objects(self, bucket: str, prefix: str = "") -> AsyncIterator[ObjectMetadata]:
        """Streaming: nunca vuelca el bucket entero a una lista en memoria,
        para que un bucket con millones de objetos no agote RAM al listarlo."""
        ...


@runtime_checkable
class MessageQueue(Protocol):
    """Cola de trabajo distribuido (frontera de crawl, jobs de
    extracción/indexación) con semántica de grupos de consumidores: varios
    workers de un mismo `group` se reparten los mensajes de un `stream` sin
    duplicarse entre sí, y cada mensaje requiere `ack` explícito o se
    redelivera (ver `ARCHITECTURE.md`, sección Redis Streams)."""

    async def ensure_group(self, stream: str, group: str) -> None:
        """Crea el grupo de consumidores si no existe todavía. Idempotente:
        llamarlo repetidas veces no falla ni duplica estado."""
        ...

    async def publish(self, stream: str, payload: Mapping[str, Any]) -> str:
        """Encola `payload` (debe ser serializable a JSON) y devuelve el
        `message_id` opaco asignado por el backend."""
        ...

    async def consume(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 10,
        block_ms: int = 5000,
    ) -> list[QueueMessage]:
        """Bloquea hasta `block_ms` milisegundos esperando hasta `count`
        mensajes nuevos para `consumer` dentro de `group`. Devuelve lista
        vacía si no llega nada en ese plazo — nunca bloquea indefinidamente."""
        ...

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        """Confirma que `message_id` se procesó correctamente; sin este ack
        el mensaje queda pendiente y es candidato a redelivery."""
        ...


@runtime_checkable
class ServiceRegistry(Protocol):
    """Registro de servicio para que el coordinador de shards (o cualquier
    pieza que haga fan-out) descubra instancias vivas dinámicamente, en vez
    de leer una lista fija de hosts/puertos en un fichero de configuración."""

    async def register(self, instance: ServiceInstance, *, ttl_seconds: float = 30.0) -> None:
        """Anuncia `instance`. Si no se renueva con `heartbeat` dentro de
        `ttl_seconds`, el registro debe dejar de devolverla en `discover`."""
        ...

    async def deregister(self, service_id: str) -> None:
        """Idempotente: dar de baja un `service_id` ya ausente no es error."""
        ...

    async def heartbeat(self, service_id: str) -> None:
        """Renueva el TTL de `service_id`. Levanta `ServiceRegistryError` si
        `service_id` no está registrado — a diferencia de `deregister`, un
        heartbeat sobre algo inexistente sí es una condición de error porque
        indica que el caller perdió su propio registro (p. ej. tras un
        reinicio del registro) y debe volver a registrarse desde cero."""
        ...

    async def discover(self, service_name: str) -> list[ServiceInstance]:
        """Devuelve solo las instancias vivas (TTL no expirado) de
        `service_name`, en cualquier orden — el llamador decide la
        estrategia de balanceo/fan-out."""
        ...
