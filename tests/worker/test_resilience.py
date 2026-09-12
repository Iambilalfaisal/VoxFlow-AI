import asyncio

import pytest
from livekit.agents.llm import LLM, ChatChunk, ChatContext, ChoiceDelta, LLMStream
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS, NOT_GIVEN

from worker.resilience import BudgetedLLM, ProviderBudget


async def test_provider_budget_limits_concurrency():
    budget = ProviderBudget("test", max_concurrent=2)
    active = 0
    peak = 0

    async def worker():
        nonlocal active, peak
        async with budget:
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.02)
            active -= 1

    await asyncio.gather(*[worker() for _ in range(5)])
    assert peak == 2


async def test_provider_budget_releases_on_exception():
    budget = ProviderBudget("test", max_concurrent=1)

    with pytest.raises(ValueError):
        async with budget:
            assert budget.in_flight == 1
            raise ValueError("boom")

    assert budget.in_flight == 0
    # Semaphore was actually released - a second acquire must not block.
    async with asyncio.timeout(1):
        async with budget:
            pass


class _FakeLLMStream(LLMStream):
    def __init__(self, llm, *, chat_ctx, tools, conn_options, chunks, error, delay):
        super().__init__(llm, chat_ctx=chat_ctx, tools=tools, conn_options=conn_options)
        self._chunks = chunks
        self._error = error
        self._delay = delay

    async def _run(self) -> None:
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._error:
            raise self._error
        for chunk in self._chunks:
            self._event_ch.send_nowait(chunk)


class _FakeLLM(LLM):
    def __init__(self, chunks=None, error=None, delay: float = 0.0):
        super().__init__()
        self._chunks = chunks or []
        self._error = error
        self._delay = delay
        self.call_count = 0

    @property
    def model(self) -> str:
        return "fake-model"

    @property
    def provider(self) -> str:
        return "fake-provider"

    def chat(
        self,
        *,
        chat_ctx,
        tools=None,
        conn_options=DEFAULT_API_CONNECT_OPTIONS,
        parallel_tool_calls=NOT_GIVEN,
        tool_choice=NOT_GIVEN,
        extra_kwargs=NOT_GIVEN,
    ) -> LLMStream:
        self.call_count += 1
        return _FakeLLMStream(
            self,
            chat_ctx=chat_ctx,
            tools=tools or [],
            conn_options=conn_options,
            chunks=self._chunks,
            error=self._error,
            delay=self._delay,
        )


async def test_budgeted_llm_forwards_chunks():
    chunk = ChatChunk(id="1", delta=ChoiceDelta(role="assistant", content="hi"))
    fake = _FakeLLM(chunks=[chunk])
    budgeted = BudgetedLLM(fake, ProviderBudget("llm", max_concurrent=2))

    async with budgeted.chat(chat_ctx=ChatContext.empty()) as stream:
        results = [c async for c in stream]

    assert results == [chunk]
    assert fake.call_count == 1


async def test_budgeted_llm_serializes_beyond_budget():
    # Each fake call "holds" the budget for `delay` seconds; with
    # max_concurrent=1, two concurrent chat() calls must not overlap.
    fake = _FakeLLM(chunks=[ChatChunk(id="1")], delay=0.05)
    budget = ProviderBudget("llm", max_concurrent=1)
    budgeted = BudgetedLLM(fake, budget)

    peak = 0

    async def one_call():
        nonlocal peak
        # Check in_flight from inside the iteration, not right after
        # __aenter__ returns: LLMStream.__init__ schedules _run() as a
        # background task via create_task, so it may not have started (and
        # thus not yet acquired the budget) the instant chat() returns.
        async with budgeted.chat(chat_ctx=ChatContext.empty()) as stream:
            async for _ in stream:
                peak = max(peak, budget.in_flight)

    await asyncio.gather(one_call(), one_call())
    assert peak == 1
    assert budget.in_flight == 0


async def test_budgeted_llm_releases_budget_on_inner_error():
    fake = _FakeLLM(error=RuntimeError("provider down"))
    budget = ProviderBudget("llm", max_concurrent=1)
    budgeted = BudgetedLLM(fake, budget)

    with pytest.raises(RuntimeError):
        async with budgeted.chat(chat_ctx=ChatContext.empty()) as stream:
            async for _ in stream:
                pass

    assert budget.in_flight == 0
