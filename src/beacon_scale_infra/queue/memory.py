"""Implementación en memoria de `MessageQueue` para desarrollo local y tests:
un log de mensajes append-only por stream, más un cursor y un conjunto de
mensajes pendientes por grupo de consumidores -- replica la semántica mínima
de Redis Streams (`XADD`/`XREADGROUP`/`XACK`) sin red ni persistencia, para
poder testear lógica de consumo/ack de forma determinista y sin levantar un
Redis real.

Los IDs de mensaje son un contador monotónico propio (`f"{seq}-0"`), nunca
derivado de `time.time()`, para que el orden de entrega sea 100% reproducible
en tests sin depender de la resolución del reloj del sistema (mismo criterio
de determinismo que `~/Desarrollo/beacon-search-engine/CLAUDE.md`, sección
2.B, exige para IDs de documento).

No es segura entre procesos: solo entre tareas `asyncio` de un mismo proceso
Python, como el resto de implementaciones locales de este paquete.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from beacon_scale_infra.errors import MessageQueueError
from beacon_scale_infra.models import QueueMessage


@dataclass
class _GroupState:
    cursor: int = 0
    pending: dict[str, int] = field(default_factory=dict)  # message_id -> delivery_count


@dataclass
class _StreamState:
    entries: list[tuple[str, Mapping[str, Any]]] = field(default_factory=list)
    next_seq: int = 0
    groups: dict[str, _GroupState] = field(default_factory=dict)


class InMemoryMessageQueue:
    def __init__(self) -> None:
        self._streams: dict[str, _StreamState] = {}
        self._condition = asyncio.Condition()

    def _stream(self, stream: str) -> _StreamState:
        return self._streams.setdefault(stream, _StreamState())

    async def ensure_group(self, stream: str, group: str) -> None:
        async with self._condition:
            self._stream(stream).groups.setdefault(group, _GroupState())

    async def publish(self, stream: str, payload: Mapping[str, Any]) -> str:
        async with self._condition:
            stream_state = self._stream(stream)
            message_id = f"{stream_state.next_seq}-0"
            stream_state.next_seq += 1
            stream_state.entries.append((message_id, dict(payload)))
            self._condition.notify_all()
            return message_id

    async def consume(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 10,
        block_ms: int = 5000,
    ) -> list[QueueMessage]:
        """`consumer` se acepta por conformidad con `MessageQueue` (el
        backend real de Redis Streams lo necesita para `XREADGROUP`), pero
        esta implementación en memoria no distingue redelivery por
        consumidor: cada mensaje se entrega una única vez a quien primero
        llame a `consume`, sin reclamo tipo `XCLAIM` en esta fase."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + block_ms / 1000
        async with self._condition:
            while True:
                messages = self._collect_new_messages(stream, group, count)
                if messages:
                    return messages
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return []
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                except TimeoutError:
                    return []

    def _collect_new_messages(self, stream: str, group: str, count: int) -> list[QueueMessage]:
        stream_state = self._streams.get(stream)
        if stream_state is None or group not in stream_state.groups:
            raise MessageQueueError(
                f"grupo de consumidores {group!r} no existe para el stream {stream!r}; "
                "llama a ensure_group primero"
            )
        group_state = stream_state.groups[group]
        collected: list[QueueMessage] = []
        while group_state.cursor < len(stream_state.entries) and len(collected) < count:
            message_id, payload = stream_state.entries[group_state.cursor]
            group_state.cursor += 1
            group_state.pending[message_id] = 1
            collected.append(QueueMessage(message_id=message_id, payload=payload, delivery_count=1))
        return collected

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        async with self._condition:
            stream_state = self._streams.get(stream)
            if stream_state is None:
                return
            group_state = stream_state.groups.get(group)
            if group_state is None:
                return
            group_state.pending.pop(message_id, None)
