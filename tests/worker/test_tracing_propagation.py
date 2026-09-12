"""Requires the local Postgres/pgbouncer containers (same as
test_history_writer.py) - the insert inside _process_batch is expected to
fail here (no matching Conversation row / no real batch semantics), which is
fine: the span this test checks is opened before that attempt and still ends
on failure, so success of the insert itself isn't what's under test.
"""

import uuid
from datetime import datetime, timezone

from opentelemetry import propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import worker.history_writer as history_writer_module
from services.queue import QueueMessage


def _event(conversation_id, content: str = "hello") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "conversation_id": str(conversation_id),
        "role": "user",
        "content": content,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


class _FakeQueue:
    async def dead_letter(self, *args, **kwargs) -> None:
        pass

    async def ack(self, *args, **kwargs) -> None:
        pass


def _in_memory_tracer(monkeypatch):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    monkeypatch.setattr(history_writer_module, "_tracer", tracer)
    return tracer, exporter


async def test_trace_context_propagates_from_publish_to_write(monkeypatch):
    tracer, exporter = _in_memory_tracer(monkeypatch)

    with tracer.start_as_current_span("history.publish") as publish_span:
        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        publish_trace_id = publish_span.get_span_context().trace_id

    entry = QueueMessage(
        id="1-1",
        payload={"events": [_event(uuid.uuid4())], "_trace": carrier},
        delivery_count=1,
    )

    await history_writer_module._process_batch(_FakeQueue(), [entry])

    write_spans = [s for s in exporter.get_finished_spans() if s.name == "history.write"]
    assert len(write_spans) == 1
    # One connected trace, not two orphans: the write span's trace ID must
    # match the publish span's, not just exist alongside it.
    assert write_spans[0].context.trace_id == publish_trace_id


async def test_missing_traceparent_starts_a_new_trace_not_an_error(monkeypatch):
    """A message with no "_trace" key (published before this field existed,
    or replayed from an old DLQ entry) must not raise - propagate.extract()
    on an empty carrier returns an empty context, so the write span just
    starts a fresh trace."""
    _tracer, exporter = _in_memory_tracer(monkeypatch)

    entry = QueueMessage(
        id="1-1", payload={"events": [_event(uuid.uuid4())]}, delivery_count=1
    )

    await history_writer_module._process_batch(_FakeQueue(), [entry])

    write_spans = [s for s in exporter.get_finished_spans() if s.name == "history.write"]
    assert len(write_spans) == 1
    assert write_spans[0].context.trace_id != 0
