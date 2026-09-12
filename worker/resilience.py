import asyncio
from typing import Any

from livekit.agents.llm import LLM, ChatContext, LLMStream, Tool, ToolChoice
from livekit.agents.types import (
    DEFAULT_API_CONNECT_OPTIONS,
    NOT_GIVEN,
    APIConnectOptions,
    NotGivenOr,
)


class ProviderBudget:
    """Caps concurrent in-flight work against one provider.

    Local asyncio.Semaphore now; the seam for a distributed Redis
    token-bucket later is this class's interface, not its implementation -
    swap the body without touching callers.
    """

    def __init__(self, name: str, max_concurrent: int):
        self._name = name
        self._sem = asyncio.Semaphore(max_concurrent)
        self._in_flight = 0

    async def __aenter__(self) -> "ProviderBudget":
        await self._sem.acquire()
        self._in_flight += 1
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._in_flight -= 1
        self._sem.release()

    @property
    def name(self) -> str:
        return self._name

    @property
    def in_flight(self) -> int:
        return self._in_flight


class BudgetedLLM(LLM):
    """Gates every chat() call on `budget` before it reaches the real LLM.

    Mirrors livekit.agents.llm.FallbackAdapter's own extension pattern (a
    thin LLM subclass whose stream's _run() does the real work) rather than
    reimplementing LLM's retry/metrics/tracing scaffolding, which the base
    LLMStream.__init__ already sets up generically.
    """

    def __init__(self, llm: LLM, budget: ProviderBudget):
        super().__init__()
        self._llm = llm
        self._budget = budget

    @property
    def model(self) -> str:
        return self._llm.model

    @property
    def provider(self) -> str:
        return self._llm.provider

    def chat(
        self,
        *,
        chat_ctx: ChatContext,
        tools: list[Tool] | None = None,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
        parallel_tool_calls: NotGivenOr[bool] = NOT_GIVEN,
        tool_choice: NotGivenOr[ToolChoice] = NOT_GIVEN,
        extra_kwargs: NotGivenOr[dict[str, Any]] = NOT_GIVEN,
    ) -> LLMStream:
        return _BudgetedLLMStream(
            self,
            inner_llm=self._llm,
            budget=self._budget,
            chat_ctx=chat_ctx,
            tools=tools or [],
            conn_options=conn_options,
            parallel_tool_calls=parallel_tool_calls,
            tool_choice=tool_choice,
            extra_kwargs=extra_kwargs,
        )

    def prewarm(self, *, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._llm.prewarm(loop=loop)

    async def aclose(self) -> None:
        await super().aclose()
        await self._llm.aclose()


class _BudgetedLLMStream(LLMStream):
    def __init__(
        self,
        llm: BudgetedLLM,
        *,
        inner_llm: LLM,
        budget: ProviderBudget,
        chat_ctx: ChatContext,
        tools: list[Tool],
        conn_options: APIConnectOptions,
        parallel_tool_calls: NotGivenOr[bool],
        tool_choice: NotGivenOr[ToolChoice],
        extra_kwargs: NotGivenOr[dict[str, Any]],
    ) -> None:
        super().__init__(llm, chat_ctx=chat_ctx, tools=tools, conn_options=conn_options)
        self._inner_llm = inner_llm
        self._budget = budget
        self._parallel_tool_calls = parallel_tool_calls
        self._tool_choice = tool_choice
        self._extra_kwargs = extra_kwargs

    async def _run(self) -> None:
        async with self._budget:
            async with self._inner_llm.chat(
                chat_ctx=self._chat_ctx,
                tools=self._tools,
                conn_options=self._conn_options,
                parallel_tool_calls=self._parallel_tool_calls,
                tool_choice=self._tool_choice,
                extra_kwargs=self._extra_kwargs,
            ) as stream:
                async for chunk in stream:
                    self._event_ch.send_nowait(chunk)
