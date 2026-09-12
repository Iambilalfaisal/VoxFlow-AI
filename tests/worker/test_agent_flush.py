"""Covers worker/agent.py's publish-retry logic.

flush_buffer() itself is a closure inside entrypoint() and needs a real
LiveKit JobContext to reach, so these tests exercise the two module-level
helpers it delegates to (_build_publish_payload, _publish_with_retry)
directly, monkeypatching the module-level `_queue` singleton with a fake.
"""

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

import worker.agent as agent_module
from worker.agent import PendingMessage


class FakeQueue:
    def __init__(self, fail_times: int = 0):
        self.fail_times = fail_times
        self.calls: list[dict] = []

    async def publish(self, stream: str, payload: dict) -> str:
        self.calls.append(payload)
        if len(self.calls) <= self.fail_times:
            raise ConnectionError("simulated redis outage")
        return "0-1"


@pytest.fixture(autouse=True)
def no_real_sleep(monkeypatch):
    async def _instant_sleep(_seconds):
        return None

    monkeypatch.setattr(agent_module.asyncio, "sleep", _instant_sleep)


def _batch(n: int = 2) -> list[PendingMessage]:
    conversation_id = uuid4()
    return [
        PendingMessage(
            conversation_id=conversation_id,
            role="user" if i % 2 == 0 else "assistant",
            content=f"message {i}",
            created_at=datetime.now(timezone.utc),
        )
        for i in range(n)
    ]


def test_build_publish_payload_is_json_serializable_with_unique_event_ids():
    batch = _batch(3)

    payload = agent_module._build_publish_payload(batch)
    json.dumps(payload)  # must not raise

    events = payload["events"]
    assert len(events) == 3
    assert [e["content"] for e in events] == [item.content for item in batch]
    assert [e["role"] for e in events] == [item.role for item in batch]
    assert all(e["conversation_id"] == str(batch[0].conversation_id) for e in events)

    event_ids = [e["event_id"] for e in events]
    assert len(set(event_ids)) == len(event_ids)  # all unique


async def test_publish_succeeds_on_first_try(monkeypatch):
    fake = FakeQueue(fail_times=0)
    monkeypatch.setattr(agent_module, "_queue", fake)

    payload = agent_module._build_publish_payload(_batch(1))
    ok = await agent_module._publish_with_retry(payload, batch_size=1)

    assert ok is True
    # _publish_with_retry adds a "_trace" carrier (trace-context propagation
    # to history_writer.py) on top of the original payload - same events,
    # plus that one extra key.
    assert len(fake.calls) == 1
    published = fake.calls[0]
    assert published["events"] == payload["events"]
    assert "traceparent" in published["_trace"]


async def test_publish_retries_then_succeeds(monkeypatch):
    max_retries = agent_module.settings.history_publish_max_retries
    fake = FakeQueue(fail_times=max_retries - 1)
    monkeypatch.setattr(agent_module, "_queue", fake)

    payload = agent_module._build_publish_payload(_batch(1))
    ok = await agent_module._publish_with_retry(payload, batch_size=1)

    assert ok is True
    assert len(fake.calls) == max_retries


async def test_publish_fails_after_max_retries_does_not_swallow_batch(monkeypatch):
    max_retries = agent_module.settings.history_publish_max_retries
    fake = FakeQueue(fail_times=max_retries)  # every attempt fails
    monkeypatch.setattr(agent_module, "_queue", fake)

    payload = agent_module._build_publish_payload(_batch(1))
    ok = await agent_module._publish_with_retry(payload, batch_size=1)

    # False here is what tells flush_buffer() to requeue the batch instead
    # of dropping it - see the call site's `if not await _publish_with_retry(...)`.
    assert ok is False
    assert len(fake.calls) == max_retries
