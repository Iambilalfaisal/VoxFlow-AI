import asyncio
import contextlib
import logging
import multiprocessing
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Run directly as `python worker/agent.py`, so the project root (for the
# `core` package) isn't on sys.path the way it would be with `python -m`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from livekit.agents import (  # noqa: E402
    Agent,
    AgentSession,
    ConversationItemAddedEvent,
    JobContext,
    WorkerOptions,
    cli,
    inference,
)
from livekit.agents.llm import ChatMessage, FallbackAdapter as LLMFallbackAdapter  # noqa: E402
from livekit.agents.stt import FallbackAdapter as STTFallbackAdapter  # noqa: E402
from livekit.agents.tts import FallbackAdapter as TTSFallbackAdapter  # noqa: E402
from livekit.plugins import silero  # noqa: E402
from opentelemetry import propagate, trace as otel_trace  # noqa: E402
from sqlalchemy import select  # noqa: E402

from core import metrics as core_metrics  # noqa: E402
from core.config import settings  # noqa: E402
from core.tracing import configure_tracing  # noqa: E402
from db.models import Conversation  # noqa: E402
from db.session import async_session_maker  # noqa: E402
from services.queue import RedisStreamQueue  # noqa: E402
from worker.resilience import BudgetedLLM, ProviderBudget  # noqa: E402

logger = logging.getLogger("voxflow.worker")

configure_tracing("voxflow-worker")
# Every job runs in its own spawned subprocess, which re-imports this file -
# only the main process may bind the metrics port, or every job subprocess
# crashes trying to rebind it (see core/metrics.py's multiprocess-mode note;
# job subprocesses still contribute metrics without serving HTTP themselves).
if settings.enable_observability and multiprocessing.current_process().name == "MainProcess":
    core_metrics.start_metrics_server(settings.metrics_port_worker)

_tracer = otel_trace.get_tracer("voxflow.worker")

# Module-level singletons, same pattern as db/session.py's `engine` - one
# shared connection pool / budget for the whole worker process, not
# per-session, so they actually bound the fleet's total concurrent usage.
_queue = RedisStreamQueue(settings.redis_url)
_llm_budget = ProviderBudget("llm", settings.llm_max_concurrent)
_stt_budget = ProviderBudget("stt", settings.stt_max_concurrent)
_tts_budget = ProviderBudget("tts", settings.tts_max_concurrent)

FLUSH_INTERVAL_SECONDS = 5
FLUSH_BATCH_SIZE = 10


def _log_availability(kind: str):
    def _handler(event) -> None:
        instance = getattr(event, kind)
        state = "available" if event.available else "UNAVAILABLE"
        logger.warning("%s provider %s is now %s", kind, instance.label, state)
        core_metrics.handle_availability_changed(kind, event.available)

    return _handler


def _build_pipeline() -> tuple[STTFallbackAdapter, LLMFallbackAdapter, TTSFallbackAdapter]:
    """Build the STT/LLM/TTS pipeline for one session.

    Every provider call goes through two layers, per CLAUDE.md §5a:
    - FallbackAdapter (from livekit-agents itself): a closed/open/half-open
      circuit breaker plus an ordered fallback chain. Reused rather than
      reimplemented - it's the framework's own machinery that AgentSession
      calls into, not a parallel structure only our code would use.
    - ProviderBudget (worker/resilience.py): a concurrency cap the SDK
      doesn't provide on its own, since FallbackAdapter only reacts to
      failures rather than throttling proactively.

    All models route through LiveKit Inference (no separate Deepgram/OpenAI/
    Cartesia API keys needed). Fallbacks are cross-provider for STT/TTS and a
    cheaper same-provider tier for LLM (the OpenAI-vs-Anthropic choice in
    CLAUDE.md §4 is still open; this doesn't relitigate it).
    """
    stt_pipeline = STTFallbackAdapter(
        [inference.STT(settings.stt_model), inference.STT(settings.stt_fallback_model)]
    )
    stt_pipeline.on("stt_availability_changed", _log_availability("stt"))

    llm_pipeline = LLMFallbackAdapter(
        [
            BudgetedLLM(inference.LLM(settings.llm_model), _llm_budget),
            BudgetedLLM(inference.LLM(settings.llm_fallback_model), _llm_budget),
        ]
    )
    llm_pipeline.on("llm_availability_changed", _log_availability("llm"))

    tts_pipeline = TTSFallbackAdapter(
        [inference.TTS(settings.tts_model), inference.TTS(settings.tts_fallback_model)]
    )
    tts_pipeline.on("tts_availability_changed", _log_availability("tts"))

    return stt_pipeline, llm_pipeline, tts_pipeline


@dataclass
class PendingMessage:
    conversation_id: uuid.UUID
    role: str
    content: str
    created_at: datetime


def _build_publish_payload(batch: list[PendingMessage]) -> dict:
    """Build the JSON-serializable payload published for one flushed batch.

    Extracted to module level (see `_publish_with_retry`) so payload shape -
    including the per-event dedup `event_id` - is directly unit-testable.
    """
    return {
        "events": [
            {
                "event_id": str(uuid.uuid4()),
                "conversation_id": str(item.conversation_id),
                "role": item.role,
                "content": item.content,
                "created_at": item.created_at.isoformat(),
            }
            for item in batch
        ]
    }


async def _publish_with_retry(payload: dict, batch_size: int) -> bool:
    """Publish `payload` to the history stream, retrying with backoff.

    Extracted to module level (out of entrypoint()'s flush_buffer closure)
    so it's unit-testable against a fake `_queue` without needing a real
    LiveKit JobContext. Returns True on success, False if every attempt
    failed - the caller decides what to do with a failed batch.
    """
    delay = settings.history_publish_retry_backoff_seconds
    with _tracer.start_as_current_span("history.publish"):
        # Carries the publishing span's context across the Redis-queue
        # boundary so history_writer.py can parent its DB-insert span to it.
        # A missing/empty carrier on the consumer side (e.g. this field
        # didn't exist on messages published before this change) just means
        # propagate.extract() returns an empty context - the consumer starts
        # a fresh trace rather than erroring, so replaying old entries is safe.
        trace_carrier: dict[str, str] = {}
        propagate.inject(trace_carrier)
        payload = {**payload, "_trace": trace_carrier}

        for attempt in range(1, settings.history_publish_max_retries + 1):
            try:
                await _queue.publish(settings.history_stream_name, payload)
                logger.info("published %d buffered transcript message(s)", batch_size)
                return True
            except Exception:
                logger.warning(
                    "publish attempt %d/%d failed for %d buffered message(s)",
                    attempt,
                    settings.history_publish_max_retries,
                    batch_size,
                    exc_info=True,
                )
                if attempt < settings.history_publish_max_retries:
                    await asyncio.sleep(delay)
                    delay *= 2

        logger.error(
            "failed to publish %d buffered message(s) after %d attempts; requeuing for next flush",
            batch_size,
            settings.history_publish_max_retries,
        )
        return False


async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect()

    # The Conversation row already exists - FastAPI creates it when it issues
    # the room token, before the client (and this job) ever joins.
    async with async_session_maker() as db:
        conversation = await db.scalar(
            select(Conversation).where(Conversation.livekit_room_name == ctx.room.name)
        )
    conversation_id = conversation.id if conversation else None
    if conversation_id is None:
        logger.error(
            "no Conversation row for room %s; transcripts will not be persisted", ctx.room.name
        )

    # Buffered/batched persistence: the transcript handler below never awaits
    # a DB call directly (it can't - it's a sync event callback, and even if
    # it were async we don't want every turn blocking on a write). It only
    # appends to this list; a separate flush_worker task drains it on a timer
    # or once it's built up enough items, so a slow or unavailable database
    # never stalls the live audio loop.
    buffer: list[PendingMessage] = []
    flush_signal = asyncio.Event()

    def on_conversation_item_added(event: ConversationItemAddedEvent) -> None:
        if conversation_id is None or not isinstance(event.item, ChatMessage):
            return
        if event.item.role not in ("user", "assistant"):
            return
        text = event.item.text_content
        if not text:
            return

        buffer.append(
            PendingMessage(
                conversation_id=conversation_id,
                role=event.item.role,
                content=text,
                created_at=datetime.fromtimestamp(event.item.created_at, tz=timezone.utc),
            )
        )
        if len(buffer) >= FLUSH_BATCH_SIZE:
            flush_signal.set()

    async def flush_buffer() -> None:
        if not buffer:
            return
        batch, buffer[:] = buffer[:], []
        payload = _build_publish_payload(batch)

        # If every publish attempt fails, requeue the batch at the front of
        # `buffer` (preserving order against anything appended meanwhile)
        # instead of discarding it, so the next flush cycle retries it. The
        # old direct-DB-write path dropped a batch on first failure, which
        # is a silent data-loss gap we don't want to carry forward now that
        # durability is the point of the queue.
        if not await _publish_with_retry(payload, len(batch)):
            buffer[0:0] = batch

    async def flush_worker() -> None:
        while True:
            try:
                await asyncio.wait_for(flush_signal.wait(), timeout=FLUSH_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass
            flush_signal.clear()
            await flush_buffer()

    flush_task = asyncio.create_task(flush_worker())

    async def on_shutdown() -> None:
        flush_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await flush_task
        await flush_buffer()

    ctx.add_shutdown_callback(on_shutdown)

    stt_pipeline, llm_pipeline, tts_pipeline = _build_pipeline()
    session = AgentSession(
        vad=silero.VAD.load(),
        stt=stt_pipeline,
        llm=llm_pipeline,
        tts=tts_pipeline,
    )
    session.on("conversation_item_added", on_conversation_item_added)
    # livekit-agents' own per-request metrics (STT/LLM/TTS duration, ttft/
    # ttfb) and error events (including 429s, via APIStatusError.status_code)
    # - fed straight into Prometheus rather than re-timing each provider call
    # ourselves. See core/metrics.py.
    session.on("metrics_collected", core_metrics.handle_metrics_collected)
    session.on("error", core_metrics.handle_provider_error)

    agent = Agent(instructions="You are VoxFlow, a helpful, concise voice assistant.")

    # STT/TTS budgets are held for the whole call (they're long-lived
    # streams, not discrete per-turn requests like the LLM) - this caps how
    # many concurrent calls this worker process will have an active STT/TTS
    # stream for. Released automatically on any exit path.
    core_metrics.ACTIVE_SESSIONS.inc()
    try:
        async with _stt_budget, _tts_budget:
            await session.start(agent=agent, room=ctx.room)
    finally:
        core_metrics.ACTIVE_SESSIONS.dec()


if __name__ == "__main__":
    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            ws_url=settings.livekit_url,
            api_key=settings.livekit_api_key,
            api_secret=settings.livekit_api_secret,
        )
    )
