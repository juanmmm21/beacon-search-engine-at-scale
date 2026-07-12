"""Tests de contrato de `RedisStreamsMessageQueue` contra `fakeredis`
(implementación en memoria fiel del protocolo Redis, incluyendo Streams):
verifican que este cliente llama a XADD/XGROUP CREATE/XREADGROUP/XACK
correctamente -- no requieren un Redis real en marcha para correr en CI."""

from __future__ import annotations

import pytest
from fakeredis import aioredis as fakeredis_aioredis

from beacon_scale_infra.errors import MessageQueueError
from beacon_scale_infra.queue.redis_streams import RedisStreamsMessageQueue


@pytest.fixture
async def queue() -> RedisStreamsMessageQueue:
    client = fakeredis_aioredis.FakeRedis(decode_responses=True)
    return RedisStreamsMessageQueue(client=client)


async def test_ensure_group_is_idempotent(queue: RedisStreamsMessageQueue) -> None:
    await queue.ensure_group("stream", "group")
    await queue.ensure_group("stream", "group")  # no debe lanzar (BUSYGROUP se traga)


async def test_consume_without_ensure_group_raises(queue: RedisStreamsMessageQueue) -> None:
    with pytest.raises(MessageQueueError):
        await queue.consume("stream", "group", "consumer-1", block_ms=10)


async def test_publish_then_consume_round_trips_json_payload(
    queue: RedisStreamsMessageQueue,
) -> None:
    await queue.ensure_group("stream", "group")
    await queue.publish("stream", {"kind": "demo", "n": 1})

    messages = await queue.consume("stream", "group", "consumer-1", block_ms=100)

    assert len(messages) == 1
    assert messages[0].payload == {"kind": "demo", "n": 1}


async def test_ack_confirms_message_and_is_safe_to_call_twice(
    queue: RedisStreamsMessageQueue,
) -> None:
    await queue.ensure_group("stream", "group")
    await queue.publish("stream", {"n": 1})
    [message] = await queue.consume("stream", "group", "consumer-1", block_ms=100)

    await queue.ack("stream", "group", message.message_id)
    await queue.ack("stream", "group", message.message_id)  # XACK repetido es un no-op


async def test_consume_returns_empty_list_when_no_new_messages(
    queue: RedisStreamsMessageQueue,
) -> None:
    await queue.ensure_group("stream", "group")
    messages = await queue.consume("stream", "group", "consumer-1", block_ms=50)
    assert messages == []
