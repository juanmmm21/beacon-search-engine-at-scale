"""Tests de comportamiento de `InMemoryMessageQueue`: determinismo de IDs,
semántica de grupos de consumidores y timeout de bloqueo sin mensajes."""

from __future__ import annotations

import asyncio
import time

import pytest

from beacon_scale_infra.errors import MessageQueueError
from beacon_scale_infra.queue.memory import InMemoryMessageQueue


@pytest.fixture
def queue() -> InMemoryMessageQueue:
    return InMemoryMessageQueue()


async def test_publish_assigns_monotonically_increasing_ids(queue: InMemoryMessageQueue) -> None:
    first_id = await queue.publish("stream", {"n": 1})
    second_id = await queue.publish("stream", {"n": 2})
    assert first_id == "0-0"
    assert second_id == "1-0"


async def test_consume_without_ensure_group_raises(queue: InMemoryMessageQueue) -> None:
    await queue.publish("stream", {"n": 1})
    with pytest.raises(MessageQueueError, match="ensure_group"):
        await queue.consume("stream", "group", "consumer-1", block_ms=10)


async def test_publish_then_consume_returns_payload(queue: InMemoryMessageQueue) -> None:
    await queue.ensure_group("stream", "group")
    await queue.publish("stream", {"kind": "demo"})

    messages = await queue.consume("stream", "group", "consumer-1", block_ms=10)

    assert len(messages) == 1
    assert messages[0].payload == {"kind": "demo"}
    assert messages[0].message_id == "0-0"


async def test_consume_respects_count_limit(queue: InMemoryMessageQueue) -> None:
    await queue.ensure_group("stream", "group")
    for n in range(5):
        await queue.publish("stream", {"n": n})

    first_batch = await queue.consume("stream", "group", "consumer-1", count=2, block_ms=10)
    second_batch = await queue.consume("stream", "group", "consumer-1", count=2, block_ms=10)

    assert [m.payload["n"] for m in first_batch] == [0, 1]
    assert [m.payload["n"] for m in second_batch] == [2, 3]


async def test_consumer_group_does_not_redeliver_already_consumed_messages(
    queue: InMemoryMessageQueue,
) -> None:
    await queue.ensure_group("stream", "group")
    await queue.publish("stream", {"n": 1})

    first_consumer_batch = await queue.consume("stream", "group", "consumer-1", block_ms=10)
    second_consumer_batch = await queue.consume("stream", "group", "consumer-2", block_ms=10)

    assert len(first_consumer_batch) == 1
    assert second_consumer_batch == []


async def test_two_independent_groups_each_see_all_messages(queue: InMemoryMessageQueue) -> None:
    await queue.ensure_group("stream", "group-a")
    await queue.ensure_group("stream", "group-b")
    await queue.publish("stream", {"n": 1})

    batch_a = await queue.consume("stream", "group-a", "consumer", block_ms=10)
    batch_b = await queue.consume("stream", "group-b", "consumer", block_ms=10)

    assert len(batch_a) == 1
    assert len(batch_b) == 1


async def test_ack_is_idempotent_and_safe_on_unknown_ids(queue: InMemoryMessageQueue) -> None:
    await queue.ensure_group("stream", "group")
    await queue.publish("stream", {"n": 1})
    [message] = await queue.consume("stream", "group", "consumer", block_ms=10)

    await queue.ack("stream", "group", message.message_id)
    await queue.ack("stream", "group", message.message_id)  # no debe lanzar
    await queue.ack("unknown-stream", "unknown-group", "0-0")  # no debe lanzar


async def test_consume_returns_empty_list_after_block_timeout_with_no_messages(
    queue: InMemoryMessageQueue,
) -> None:
    await queue.ensure_group("stream", "group")

    started = time.monotonic()
    messages = await queue.consume("stream", "group", "consumer", block_ms=50)
    elapsed = time.monotonic() - started

    assert messages == []
    assert elapsed >= 0.05


async def test_consume_wakes_up_as_soon_as_a_message_is_published(
    queue: InMemoryMessageQueue,
) -> None:
    await queue.ensure_group("stream", "group")

    async def publish_soon() -> None:
        await asyncio.sleep(0.05)
        await queue.publish("stream", {"n": 1})

    publisher = asyncio.create_task(publish_soon())
    started = time.monotonic()
    messages = await queue.consume("stream", "group", "consumer", block_ms=5000)
    elapsed = time.monotonic() - started
    await publisher

    assert len(messages) == 1
    assert elapsed < 1.0
