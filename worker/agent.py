import asyncio
import contextlib
import logging
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
)
from livekit.agents.llm import ChatMessage  # noqa: E402
from livekit.plugins import silero  # noqa: E402
from sqlalchemy import select  # noqa: E402

from core.config import settings  # noqa: E402
from db.models import Conversation, Message  # noqa: E402
from db.session import async_session_maker  # noqa: E402

logger = logging.getLogger("voxflow.worker")

# Uses LiveKit Inference (https://docs.livekit.io) for the STT -> LLM -> TTS
# pipeline: model strings are routed and billed through LiveKit Cloud, so no
# separate Deepgram/OpenAI/Cartesia accounts or API keys are needed while
# we're on free tiers. Swap the strings below for direct provider plugins
# later if we outgrow LiveKit Inference's included usage.
STT_MODEL = "deepgram/nova-3:en"
LLM_MODEL = "openai/gpt-4.1-mini"
TTS_MODEL = "cartesia/sonic-3:6f84f4b8-58a2-430c-8c79-688dad597532"

FLUSH_INTERVAL_SECONDS = 5
FLUSH_BATCH_SIZE = 10


@dataclass
class PendingMessage:
    conversation_id: uuid.UUID
    role: str
    content: str
    created_at: datetime


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
        try:
            async with async_session_maker() as db:
                async with db.begin():
                    db.add_all(
                        Message(
                            conversation_id=item.conversation_id,
                            role=item.role,
                            content=item.content,
                            created_at=item.created_at,
                        )
                        for item in batch
                    )
            logger.info("persisted %d buffered transcript message(s)", len(batch))
        except Exception:
            logger.exception("failed to persist %d buffered transcript message(s)", len(batch))

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

    session = AgentSession(
        vad=silero.VAD.load(),
        stt=STT_MODEL,
        llm=LLM_MODEL,
        tts=TTS_MODEL,
    )
    session.on("conversation_item_added", on_conversation_item_added)

    agent = Agent(instructions="You are VoxFlow, a helpful, concise voice assistant.")

    await session.start(agent=agent, room=ctx.room)


if __name__ == "__main__":
    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            ws_url=settings.livekit_url,
            api_key=settings.livekit_api_key,
            api_secret=settings.livekit_api_secret,
        )
    )
