"""Tipos de datos compartidos por las tres piezas de sustrato.

Estos tipos no representan documentos, postings ni scores de ningún
subproyecto de `beacon-search-engine` — son puramente de infraestructura
(un mensaje de cola, una instancia de servicio). Las fases posteriores que sí
mueven datos de dominio (páginas crawleadas, documentos extraídos) definen su
propio payload como `Mapping[str, Any]` serializable a JSON dentro de
`QueueMessage.payload`, igual que el resto del ecosistema nunca comparte
imports de modelos entre repos (ver `AGENTS.md`).
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class QueueMessage:
    """Un mensaje ya entregado por `MessageQueue.consume`.

    `message_id` es el identificador nativo del backend (el ID de entrada de
    Redis Streams, p. ej. `"1700000000000-0"`) — se trata como una cadena
    opaca en el protocolo para no acoplar el contrato a la sintaxis de un
    backend concreto; `ack` lo recibe tal cual se entregó.
    """

    message_id: str
    payload: Mapping[str, Any]
    delivery_count: int = 1


@dataclass(frozen=True, slots=True)
class ServiceInstance:
    """Una instancia concreta de un servicio descubrible (p. ej. un shard del
    índice distribuido) anunciándose en el registro."""

    service_id: str
    service_name: str
    host: str
    port: int
    metadata: Mapping[str, str] = field(default_factory=dict)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def __post_init__(self) -> None:
        if not self.service_id:
            raise ValueError("service_id no puede estar vacío")
        if not self.service_name:
            raise ValueError("service_name no puede estar vacío")
        if not (0 < self.port <= 65535):
            raise ValueError(f"port fuera de rango: {self.port}")


@dataclass(frozen=True, slots=True)
class ObjectMetadata:
    """Metadatos de un objeto ya almacenado, devueltos tras un `put_object`
    o al listar un bucket — nunca incluye el contenido en sí."""

    key: str
    size_bytes: int
    content_type: str
    last_modified_epoch_seconds: float = field(default_factory=time.time)
