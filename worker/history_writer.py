import asyncio
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

# Run directly as `python worker/history_writer.py`, so the project root (for
# the `core` package) isn't on sys.path the way it would be with `python -m`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opentelemetry import propagate, trace as otel_trace  # noqa: E402

from core import metrics as core_metrics  # noqa: E402
from core.config import settings  # noqa: E402
from core.tracing import configure_tracing  # noqa: E402
from db.models import Message  # noqa: E402
from db.session import async_session_maker  # noqa: E402
from services.queue import QueueMessage, RedisStreamQueue  # noqa: E402

logger = logging.getLogger("voxflow.history_writer")

configure_tracing("voxflow-history-writer")
if settings.enable_observability:
    core_metrics.start_metrics_server(settings.metrics_port_history_writer)

_tracer = otel_trace.get_tracer("voxflow.history_writer")

# Phase 1 assumption: exactly one history-writer instance runs at a time. The
# reclaim loop below and the "one transaction per poll" batching in
# _process_batch are only correct under this assumption - two writer
# replicas would XAUTOCLAIM and race each other's pending entries, double-
# processing or fighting over reclaims. See docker-compose.yml, which runs a
# single instance of this service. Multi-writer support is explicitly
# deferred, not designed for here.


async def run() -> None:
    queue = RedisStreamQueue(settings.redis_url)
    await queue.ensure_group(settings.history_stream_name, settings.history_consumer_group)
    consumer = f"writer-{uuid.uuid4().hex[:8]}"
    logger.info("history_writer starting as consumer %s", consumer)

    while True:
        # XAUTOCLAIM entries idle past the threshold first (e.g. a previous
        # writer crashed after XREADGROUP but before ack/dead-letter), then
        # pull newly-published entries. Both go through the same
        # insert-and-ack path below - a reclaimed entry always gets an
        # actual retry attempt, never just a delivery-count bump.
        reclaimed = await queue.reclaim(
            settings.history_stream_name,
            settings.history_consumer_group,
            consumer,
            min_idle_ms=settings.history_writer_min_idle_ms,
            count=settings.history_writer_batch_size,
        )
        consumed = await queue.consume(
            settings.history_stream_name,
            settings.history_consumer_group,
            consumer,
            count=settings.history_writer_batch_size,
            block_ms=settings.history_writer_block_ms,
        )

        entries = reclaimed + consumed
        if entries:
            await _process_batch(queue, entries)

        # XACK doesn't shrink the stream - trim it here so it doesn't grow
        # forever regardless of whether the writer is keeping up (see
        # RedisStreamQueue.trim's docstring for why this is safe).
        await queue.trim(
            settings.history_stream_name,
            settings.history_consumer_group,
            fallback_maxlen=settings.history_stream_trim_maxlen,
        )

        # Cadence follows the poll loop itself (at most every block_ms while
        # idle) rather than a separate timer - cheap enough (4 Redis calls)
        # not to need its own schedule, and it's a leading indicator so
        # staleness up to one poll cycle is acceptable.
        dlq_stream = f"{settings.history_stream_name}:dlq"
        core_metrics.HISTORY_STREAM_DEPTH.labels(stream=settings.history_stream_name).set(
            await queue.stream_len(settings.history_stream_name)
        )
        core_metrics.HISTORY_DLQ_DEPTH.labels(stream=settings.history_stream_name).set(
            await queue.stream_len(dlq_stream)
        )
        core_metrics.HISTORY_STREAM_PENDING.labels(stream=settings.history_stream_name).set(
            await queue.pending_count(settings.history_stream_name, settings.history_consumer_group)
        )


async def _process_batch(queue: RedisStreamQueue, entries: list[QueueMessage]) -> None:
    messages = [
        Message(
            conversation_id=uuid.UUID(event["conversation_id"]),
            role=event["role"],
            content=event["content"],
            created_at=datetime.fromisoformat(event["created_at"]),
            event_id=event.get("event_id"),
        )
        for entry in entries
        for event in entry.payload.get("events", [])
    ]

    # Trace context: parented to the first entry's publish span, if any. A
    # batch can carry entries from several originating publishes, but one DB
    # transaction can only have one parent - this links the write to a
    # representative session rather than every one of them. Entries with no
    # "_trace" (e.g. published before this field existed) make extract()
    # return an empty context, so the span below just starts a fresh trace
    # instead of erroring.
    trace_carrier = entries[0].payload.get("_trace", {}) if entries else {}
    trace_ctx = propagate.extract(trace_carrier)

    start = time.monotonic()
    try:
        with _tracer.start_as_current_span("history.write", context=trace_ctx):
            # One self-contained transaction per poll (pgBouncer-safe) - same
            # batched-insert pattern already used in worker/agent.py.
            async with async_session_maker() as db:
                async with db.begin():
                    db.add_all(messages)
    except Exception:
        logger.exception(
            "failed to insert %d event(s) from %d stream entr(y/ies)", len(messages), len(entries)
        )
        # DLQ granularity is per-stream-entry: one malformed event in one
        # entry fails this whole transaction, so every entry in this poll
        # gets redelivered together, not just the poison one - until the
        # poison entry's own delivery_count individually exceeds the
        # threshold and only it is dead-lettered. Per-event transactions
        # would avoid this but conflict with the batched-insert requirement.
        for entry in entries:
            if entry.delivery_count > settings.history_writer_max_deliveries:
                await queue.dead_letter(
                    settings.history_stream_name,
                    settings.history_consumer_group,
                    entry,
                    reason="insert failed after max deliveries",
                )
                core_metrics.HISTORY_WRITER_DEAD_LETTERED_TOTAL.inc()
                logger.error(
                    "dead-lettered entry %s after %d delivery attempts",
                    entry.id,
                    entry.delivery_count,
                )
            # else: leave unacked - it stays pending and gets reclaimed and
            # retried on a future poll once idle past min_idle_ms again.
        return

    core_metrics.HISTORY_WRITER_BATCH_LATENCY_SECONDS.observe(time.monotonic() - start)
    core_metrics.HISTORY_WRITER_ROWS_TOTAL.inc(len(messages))

    for entry in entries:
        await queue.ack(settings.history_stream_name, settings.history_consumer_group, entry.id)
    logger.info("persisted %d event(s) from %d stream entr(y/ies)", len(messages), len(entries))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
