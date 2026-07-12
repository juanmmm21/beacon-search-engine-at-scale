"""Implementación real de `MessageQueue` sobre Redis Streams (`redis.asyncio`),
usando `XADD`/`XGROUP CREATE`/`XREADGROUP`/`XACK` -- ver `ARCHITECTURE.md`,
sección "Cola de mensajes", para por qué Redis Streams y no Kafka al volumen
objetivo de esta fase (unos pocos millones de páginas, no miles de millones
de eventos).

El payload se serializa a JSON dentro de un único campo (`"json"`) del
entry de Redis en vez de mapear cada clave del payload a un campo de hash
de Redis: así el contrato de `MessageQueue.publish` acepta cualquier
`Mapping[str, Any]` (incluyendo valores anidados) sin quedar limitado a los
valores planos tipo string que exige un hash de Redis nativo.

No se calcula `delivery_count` real (requeriría una llamada `XPENDING`
adicional por lote): se deja en `1` de forma explícita hasta que una fase
posterior necesite lógica de reintento/`XCLAIM`, momento en el que sí
merece la pena ese round-trip extra.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import redis.asyncio as redis_asyncio
from redis.exceptions import RedisError, ResponseError

from beacon_scale_infra.errors import MessageQueueError
from beacon_scale_infra.models import QueueMessage


class RedisStreamsMessageQueue:
    def __init__(self, *, client: redis_asyncio.Redis) -> None:
        self._client = client

    @classmethod
    def from_url(cls, url: str) -> RedisStreamsMessageQueue:
        return cls(client=redis_asyncio.Redis.from_url(url, decode_responses=True))

    async def ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._client.xgroup_create(stream, group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise MessageQueueError(
                    f"fallo al crear el grupo {group!r} en {stream!r}: {exc}"
                ) from exc
        except RedisError as exc:
            raise MessageQueueError(
                f"fallo al crear el grupo {group!r} en {stream!r}: {exc}"
            ) from exc

    async def publish(self, stream: str, payload: Mapping[str, Any]) -> str:
        try:
            message_id = await self._client.xadd(stream, {"json": _encode_payload(payload)})
        except RedisError as exc:
            raise MessageQueueError(f"fallo al publicar en {stream!r}: {exc}") from exc
        return str(message_id)

    async def consume(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 10,
        block_ms: int = 5000,
    ) -> list[QueueMessage]:
        try:
            response = await self._client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=count,
                block=block_ms,
            )
        except RedisError as exc:
            raise MessageQueueError(f"fallo al consumir de {stream!r}/{group!r}: {exc}") from exc
        if not response:
            return []
        messages: list[QueueMessage] = []
        for _stream_name, entries in response:
            for message_id, fields in entries:
                messages.append(
                    QueueMessage(
                        message_id=message_id,
                        payload=_decode_payload(fields),
                        delivery_count=1,
                    )
                )
        return messages

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        try:
            await self._client.xack(stream, group, message_id)
        except RedisError as exc:
            raise MessageQueueError(
                f"fallo al confirmar {message_id!r} en {stream!r}/{group!r}: {exc}"
            ) from exc

    async def aclose(self) -> None:
        await self._client.aclose()


def _encode_payload(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True)


def _decode_payload(fields: Mapping[str, str]) -> dict[str, Any]:
    raw = fields.get("json")
    if raw is None:
        raise MessageQueueError("mensaje sin campo 'json': formato de entry inesperado")
    decoded: dict[str, Any] = json.loads(raw)
    return decoded
