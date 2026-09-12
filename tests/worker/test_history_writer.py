"""Requires the local Redis + Postgres/pgbouncer containers, AND the
event_id migration applied against them first:

    docker compose up -d redis postgres pgbouncer
    alembic upgrade head
    pytest tests/worker/test_history_writer.py -v

Base.metadata.create_all is used for schema setup rather than Alembic (it's
a no-op here since the tables already exist), but the `messages.event_id`
column added in alembic/versions/6b0196360d98_add_event_id_to_messages.py
must already be present on the real table for inserts below to succeed -
create_all does not ALTER existing tables to add missing columns.
"""

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

import worker.history_writer as history_writer_module
from core import metrics as core_metrics
from core.config import settings
from db.models import Base, Conversation, Message, Organization, User
from db.session import async_session_maker
from services.queue import RedisStreamQueue


def _counter_value(counter, **labels) -> float:
    for metric in counter.collect():
        for sample in metric.samples:
            if sample.name.endswith("_total") and sample.labels == labels:
                return sample.value
    return 0.0


def _histogram_count(histogram) -> float:
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name.endswith("_count"):
                return sample.value
    return 0.0


@pytest.fixture(scope="module", autouse=True)
async def _ensure_schema():
    # DDL bypasses pgbouncer, same as alembic/env.py - safe here since it's a
    # no-op (tables already exist), but matches the transaction-pooling
    # constraint on principle.
    ddl_engine = create_async_engine(settings.migrations_database_url)
    async with ddl_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await ddl_engine.dispose()


@pytest.fixture(autouse=True)
def _isolated_settings(monkeypatch):
    stream = f"test:history:{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(history_writer_module.settings, "history_stream_name", stream)
    # Reclaim immediately instead of waiting out the real 30s default.
    monkeypatch.setattr(history_writer_module.settings, "history_writer_min_idle_ms", 0)
    return stream


@pytest.fixture
async def queue():
    q = RedisStreamQueue(settings.redis_url)
    yield q
    stream = history_writer_module.settings.history_stream_name
    await q._redis.delete(stream, f"{stream}:dlq")
    await q._redis.aclose()


@pytest.fixture
async def conversation():
    async with async_session_maker() as db:
        org = Organization(name=f"test-org-{uuid.uuid4().hex[:8]}")
        db.add(org)
        await db.flush()
        user = User(
            org_id=org.id, email=f"{uuid.uuid4().hex[:8]}@test.local", hashed_password="x"
        )
        db.add(user)
        await db.flush()
        conv = Conversation(user_id=user.id, livekit_room_name=f"test-room-{uuid.uuid4().hex[:8]}")
        db.add(conv)
        await db.commit()
        conv_id, user_id, org_id = conv.id, user.id, org.id

    yield conv_id

    async with async_session_maker() as db:
        await db.execute(delete(Message).where(Message.conversation_id == conv_id))
        await db.execute(delete(Conversation).where(Conversation.id == conv_id))
        await db.execute(delete(User).where(User.id == user_id))
        await db.execute(delete(Organization).where(Organization.id == org_id))
        await db.commit()


def _event(conversation_id, content: str = "hello") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "conversation_id": str(conversation_id),
        "role": "user",
        "content": content,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


async def _drive_loop(queue: RedisStreamQueue, consumer: str, iterations: int = 1) -> None:
    stream = history_writer_module.settings.history_stream_name
    group = history_writer_module.settings.history_consumer_group
    min_idle_ms = history_writer_module.settings.history_writer_min_idle_ms
    for _ in range(iterations):
        reclaimed = await queue.reclaim(stream, group, consumer, min_idle_ms=min_idle_ms, count=10)
        consumed = await queue.consume(stream, group, consumer, count=10, block_ms=100)
        entries = reclaimed + consumed
        if entries:
            await history_writer_module._process_batch(queue, entries)


async def test_insert_then_ack(queue, conversation):
    stream = history_writer_module.settings.history_stream_name
    group = history_writer_module.settings.history_consumer_group
    await queue.ensure_group(stream, group)
    await queue.publish(stream, {"events": [_event(conversation)]})

    await _drive_loop(queue, "writer-1", iterations=1)

    async with async_session_maker() as db:
        rows = list(
            await db.scalars(select(Message).where(Message.conversation_id == conversation))
        )
    assert len(rows) == 1
    assert rows[0].content == "hello"

    still_pending = await queue.reclaim(stream, group, "writer-2", min_idle_ms=0, count=10)
    assert still_pending == []


async def test_failed_insert_is_not_acked(queue):
    stream = history_writer_module.settings.history_stream_name
    group = history_writer_module.settings.history_consumer_group
    poison_conversation_id = uuid.uuid4()  # no matching Conversation row -> FK violation
    await queue.ensure_group(stream, group)
    await queue.publish(stream, {"events": [_event(poison_conversation_id)]})

    await _drive_loop(queue, "writer-1", iterations=1)

    still_pending = await queue.reclaim(stream, group, "writer-2", min_idle_ms=0, count=10)
    assert len(still_pending) == 1


async def test_threshold_exceeded_lands_in_dlq_via_real_reclaim(queue, monkeypatch):
    monkeypatch.setattr(history_writer_module.settings, "history_writer_max_deliveries", 2)

    stream = history_writer_module.settings.history_stream_name
    group = history_writer_module.settings.history_consumer_group
    poison_conversation_id = uuid.uuid4()  # every insert attempt fails (FK violation)
    await queue.ensure_group(stream, group)
    await queue.publish(stream, {"events": [_event(poison_conversation_id)]})

    # Drives real XAUTOCLAIM reclaims across iterations (delivery_count
    # incremented by Redis itself each time) until it crosses the
    # max_deliveries threshold - not a synthetic delivery-count injection.
    await _drive_loop(queue, "writer-1", iterations=5)

    still_pending = await queue.reclaim(stream, group, "writer-2", min_idle_ms=0, count=10)
    assert still_pending == []

    dlq_entries = await queue._redis.xrange(f"{stream}:dlq")
    assert len(dlq_entries) == 1


async def test_insert_updates_throughput_metrics(queue, conversation):
    stream = history_writer_module.settings.history_stream_name
    group = history_writer_module.settings.history_consumer_group
    await queue.ensure_group(stream, group)
    await queue.publish(stream, {"events": [_event(conversation), _event(conversation)]})

    rows_before = _counter_value(core_metrics.HISTORY_WRITER_ROWS_TOTAL)
    batches_before = _histogram_count(core_metrics.HISTORY_WRITER_BATCH_LATENCY_SECONDS)

    await _drive_loop(queue, "writer-1", iterations=1)

    assert _counter_value(core_metrics.HISTORY_WRITER_ROWS_TOTAL) == rows_before + 2
    assert (
        _histogram_count(core_metrics.HISTORY_WRITER_BATCH_LATENCY_SECONDS) == batches_before + 1
    )


async def test_dead_letter_increments_counter(queue, monkeypatch):
    monkeypatch.setattr(history_writer_module.settings, "history_writer_max_deliveries", 2)

    stream = history_writer_module.settings.history_stream_name
    group = history_writer_module.settings.history_consumer_group
    poison_conversation_id = uuid.uuid4()  # every insert attempt fails (FK violation)
    await queue.ensure_group(stream, group)
    await queue.publish(stream, {"events": [_event(poison_conversation_id)]})

    dead_lettered_before = _counter_value(core_metrics.HISTORY_WRITER_DEAD_LETTERED_TOTAL)

    await _drive_loop(queue, "writer-1", iterations=5)

    assert (
        _counter_value(core_metrics.HISTORY_WRITER_DEAD_LETTERED_TOTAL)
        == dead_lettered_before + 1
    )
