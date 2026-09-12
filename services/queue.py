import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

import redis.asyncio as redis
from redis.exceptions import ResponseError


@dataclass
class QueueMessage:
    id: str
    payload: dict
    delivery_count: int


class MessageQueue(Protocol):
    """Publish/consume/dead-letter interface for the history-event queue.

    `RedisStreamQueue` is the only implementation today. The interface is
    shaped so a managed SQS/Kafka implementation can drop in later behind it
    without worker/agent.py or worker/history_writer.py changing.
    """

    async def ensure_group(self, stream: str, group: str) -> None: ...

    async def publish(self, stream: str, payload: dict) -> str: ...

    async def consume(
        self, stream: str, group: str, consumer: str, count: int, block_ms: int
    ) -> list[QueueMessage]: ...

    async def reclaim(
        self, stream: str, group: str, consumer: str, min_idle_ms: int, count: int
    ) -> list[QueueMessage]: ...

    async def ack(self, stream: str, group: str, message_id: str) -> None: ...

    async def dead_letter(
        self, stream: str, group: str, message: QueueMessage, reason: str
    ) -> None: ...


class RedisStreamQueue:
    """MessageQueue backed by Redis Streams + a consumer group.

    Fields on a stream entry are str->str, so the whole payload is carried as
    one JSON-encoded field ("data") rather than mapped key-by-key.
    """

    def __init__(self, redis_url: str):
        # redis-py defaults socket_timeout to 5s (redis._defaults.DEFAULT_SOCKET_TIMEOUT).
        # XREADGROUP's BLOCK tells the *server* to hold the connection open
        # for up to block_ms waiting for new entries - with the client-side
        # default in place, a block_ms at or above 5000 races that same
        # 5s client socket timeout and the read gets killed out from under
        # itself. Disable it here so only our own block_ms governs the wait.
        self._redis = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=None)

    async def ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(stream, group, id="$", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish(self, stream: str, payload: dict) -> str:
        return await self._redis.xadd(stream, {"data": json.dumps(payload)})

    async def consume(
        self, stream: str, group: str, consumer: str, count: int, block_ms: int
    ) -> list[QueueMessage]:
        response = await self._redis.xreadgroup(
            group, consumer, {stream: ">"}, count=count, block=block_ms
        )
        if not response:
            return []
        # A freshly-delivered entry always has delivery_count 1 - Redis only
        # increments it once the entry is reclaimed (see `reclaim`).
        return [
            QueueMessage(id=entry_id, payload=json.loads(fields["data"]), delivery_count=1)
            for _stream_name, entries in response
            for entry_id, fields in entries
        ]

    async def reclaim(
        self, stream: str, group: str, consumer: str, min_idle_ms: int, count: int
    ) -> list[QueueMessage]:
        # XAUTOCLAIM reassigns ownership of entries idle longer than
        # min_idle_ms (e.g. a writer crashed after XREADGROUP but before
        # ack/dead-letter). It does not retry anything itself - the caller
        # must feed the returned entries through the same insert-and-ack path
        # used for normally-consumed entries.
        _next_start, claimed, _deleted = await self._redis.xautoclaim(
            stream, group, consumer, min_idle_time=min_idle_ms, start_id="0-0", count=count
        )
        if not claimed:
            return []

        # Redis increments delivery count on every XCLAIM/XAUTOCLAIM - read it
        # back via XPENDING rather than tracking it ourselves.
        pending = await self._redis.xpending_range(
            stream, group, min="-", max="+", count=count, consumername=consumer
        )
        delivery_counts = {entry["message_id"]: entry["times_delivered"] for entry in pending}

        return [
            QueueMessage(
                id=entry_id,
                payload=json.loads(fields["data"]),
                delivery_count=delivery_counts.get(entry_id, 1),
            )
            for entry_id, fields in claimed
        ]

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        await self._redis.xack(stream, group, message_id)

    async def dead_letter(
        self, stream: str, group: str, message: QueueMessage, reason: str
    ) -> None:
        dlq_stream = f"{stream}:dlq"
        dlq_payload = {
            **message.payload,
            "_dlq": {
                "original_stream": stream,
                "original_id": message.id,
                "delivery_count": message.delivery_count,
                "reason": reason,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            },
        }
        await self._redis.xadd(dlq_stream, {"data": json.dumps(dlq_payload)})
        # Ack the original so it stops being redelivered/reclaimable.
        await self._redis.xack(stream, group, message.id)

    async def stream_len(self, stream: str) -> int:
        """XLEN - observability-only, not part of the MessageQueue interface
        (a future SQS/Kafka backend has no equivalent concept of stream
        length in this sense)."""
        return await self._redis.xlen(stream)

    async def pending_count(self, stream: str, group: str) -> int:
        """Count of delivered-but-unacked entries (XPENDING summary form),
        for the history-writer's consumer-lag gauge."""
        summary = await self._redis.xpending(stream, group)
        return summary["pending"] if summary else 0

    async def trim(self, stream: str, group: str, fallback_maxlen: int) -> None:
        """Bound stream growth. XACK does not delete entries - only
        XTRIM/XDEL do - so without this, `stream` grows forever regardless
        of whether entries are acked.

        Anchored to the oldest still-pending (unacked) entry when one
        exists: `XTRIM MINID` only deletes entries older than that anchor,
        so it can never remove something not yet safely processed -
        including a stuck poison entry a future delivery might still
        dead-letter. When nothing is pending, there's no such anchor, so
        this falls back to an approximate MAXLEN cap purely to bound memory
        growth, not to enforce a processing guarantee.
        """
        summary = await self._redis.xpending(stream, group)
        min_pending_id = summary.get("min") if summary else None
        if min_pending_id is not None:
            await self._redis.xtrim(stream, minid=min_pending_id, approximate=True)
        else:
            await self._redis.xtrim(stream, maxlen=fallback_maxlen, approximate=True)
