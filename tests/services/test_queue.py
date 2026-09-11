"""Requires the local Redis container: `docker compose up -d redis`."""

import json
import uuid

import pytest

from services.queue import RedisStreamQueue

REDIS_URL = "redis://localhost:6379/0"
GROUP = "test-group"


@pytest.fixture
async def queue():
    q = RedisStreamQueue(REDIS_URL)
    yield q
    await q._redis.aclose()


@pytest.fixture
def stream(request):
    name = f"test:queue:{uuid.uuid4().hex[:8]}"
    return name


async def _cleanup(queue: RedisStreamQueue, stream: str) -> None:
    await queue._redis.delete(stream, f"{stream}:dlq")


async def test_publish_consume_round_trip(queue, stream):
    await queue.ensure_group(stream, GROUP)
    await queue.publish(stream, {"hello": "world"})

    messages = await queue.consume(stream, GROUP, "consumer-1", count=10, block_ms=1000)

    assert len(messages) == 1
    assert messages[0].payload == {"hello": "world"}
    assert messages[0].delivery_count == 1

    await _cleanup(queue, stream)


async def test_ensure_group_is_idempotent(queue, stream):
    await queue.ensure_group(stream, GROUP)
    await queue.ensure_group(stream, GROUP)  # must not raise BUSYGROUP

    await _cleanup(queue, stream)


async def test_reclaim_reports_incremented_delivery_count(queue, stream):
    await queue.ensure_group(stream, GROUP)
    await queue.publish(stream, {"n": 1})

    # consumer-1 reads it but never acks (simulating a crashed writer).
    consumed = await queue.consume(stream, GROUP, "consumer-1", count=10, block_ms=1000)
    assert len(consumed) == 1
    assert consumed[0].delivery_count == 1

    # consumer-2 reclaims it once it's idle past min_idle_ms=0.
    reclaimed = await queue.reclaim(stream, GROUP, "consumer-2", min_idle_ms=0, count=10)
    assert len(reclaimed) == 1
    assert reclaimed[0].id == consumed[0].id
    assert reclaimed[0].delivery_count == 2

    await queue.ack(stream, GROUP, reclaimed[0].id)
    await _cleanup(queue, stream)


async def test_dead_letter_moves_message_and_acks_original(queue, stream):
    await queue.ensure_group(stream, GROUP)
    await queue.publish(stream, {"poison": True})

    [message] = await queue.consume(stream, GROUP, "consumer-1", count=10, block_ms=1000)
    await queue.dead_letter(stream, GROUP, message, reason="test failure")

    # No longer pending/reclaimable on the original stream.
    still_pending = await queue.reclaim(stream, GROUP, "consumer-2", min_idle_ms=0, count=10)
    assert still_pending == []

    # Landed on the DLQ stream with failure metadata attached.
    dlq_entries = await queue._redis.xrange(f"{stream}:dlq")
    assert len(dlq_entries) == 1
    _dlq_id, fields = dlq_entries[0]
    dlq_payload = json.loads(fields["data"])
    assert dlq_payload["poison"] is True
    assert dlq_payload["_dlq"]["reason"] == "test failure"
    assert dlq_payload["_dlq"]["original_id"] == message.id

    await _cleanup(queue, stream)
