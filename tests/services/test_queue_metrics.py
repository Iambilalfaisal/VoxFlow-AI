"""Requires the local Redis container: `docker compose up -d redis`."""

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
    return f"test:queue-metrics:{uuid.uuid4().hex[:8]}"


async def _cleanup(queue: RedisStreamQueue, stream: str) -> None:
    await queue._redis.delete(stream, f"{stream}:dlq")


async def test_stream_len_reflects_unacked_and_acked_entries(queue, stream):
    await queue.ensure_group(stream, GROUP)
    assert await queue.stream_len(stream) == 0

    await queue.publish(stream, {"n": 1})
    await queue.publish(stream, {"n": 2})
    assert await queue.stream_len(stream) == 2

    # XLEN counts stream entries regardless of ack state.
    [msg, _] = await queue.consume(stream, GROUP, "consumer-1", count=10, block_ms=1000)
    await queue.ack(stream, GROUP, msg.id)
    assert await queue.stream_len(stream) == 2

    await _cleanup(queue, stream)


async def test_stream_len_on_dlq_stream(queue, stream):
    await queue.ensure_group(stream, GROUP)
    await queue.publish(stream, {"poison": True})
    [message] = await queue.consume(stream, GROUP, "consumer-1", count=10, block_ms=1000)

    await queue.dead_letter(stream, GROUP, message, reason="test failure")

    assert await queue.stream_len(f"{stream}:dlq") == 1
    await _cleanup(queue, stream)


async def test_pending_count_tracks_unacked_entries(queue, stream):
    await queue.ensure_group(stream, GROUP)
    assert await queue.pending_count(stream, GROUP) == 0

    await queue.publish(stream, {"n": 1})
    [message] = await queue.consume(stream, GROUP, "consumer-1", count=10, block_ms=1000)
    assert await queue.pending_count(stream, GROUP) == 1

    await queue.ack(stream, GROUP, message.id)
    assert await queue.pending_count(stream, GROUP) == 0

    await _cleanup(queue, stream)


async def test_trim_never_deletes_a_still_pending_entry(queue, stream):
    """The safety property that matters: a stuck/poison entry (delivered but
    not yet acked or dead-lettered) must survive trim() no matter how much
    newer, fully-acked traffic accumulates around it."""
    await queue.ensure_group(stream, GROUP)
    await queue.publish(stream, {"stuck": True})
    [stuck] = await queue.consume(stream, GROUP, "consumer-1", count=10, block_ms=1000)
    # left unacked - simulates a poison/slow entry still awaiting resolution

    # Enough subsequent fully-acked traffic to actually exercise trimming
    # (a handful of entries may all live in one small Redis radix node and
    # never get physically removed either way).
    for i in range(300):
        await queue.publish(stream, {"n": i})
    consumed = await queue.consume(stream, GROUP, "consumer-1", count=300, block_ms=1000)
    for msg in consumed:
        await queue.ack(stream, GROUP, msg.id)

    await queue.trim(stream, GROUP, fallback_maxlen=1)

    # Must still be fully reclaimable - not just present in XRANGE, but
    # intact from the consumer group's perspective.
    reclaimed = await queue.reclaim(stream, GROUP, "consumer-2", min_idle_ms=0, count=10)
    assert [m.id for m in reclaimed] == [stuck.id]

    await queue.ack(stream, GROUP, stuck.id)
    await _cleanup(queue, stream)


async def test_trim_bounds_growth_when_nothing_pending(queue, stream):
    await queue.ensure_group(stream, GROUP)
    for i in range(500):
        await queue.publish(stream, {"n": i})
    consumed = await queue.consume(stream, GROUP, "consumer-1", count=500, block_ms=1000)
    for msg in consumed:
        await queue.ack(stream, GROUP, msg.id)
    assert await queue.pending_count(stream, GROUP) == 0

    await queue.trim(stream, GROUP, fallback_maxlen=10)

    # Approximate trimming won't necessarily hit exactly 10, but with
    # nothing pending as an anchor it must fall back to the maxlen cap and
    # meaningfully shrink the stream, not leave it at 500.
    assert await queue.stream_len(stream) < 500

    await _cleanup(queue, stream)
