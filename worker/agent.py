from livekit.agents import Agent, AgentSession, JobContext, WorkerOptions, cli
from livekit.plugins import silero

# Uses LiveKit Inference (https://docs.livekit.io) for the STT -> LLM -> TTS
# pipeline: model strings are routed and billed through LiveKit Cloud, so no
# separate Deepgram/OpenAI/Cartesia accounts or API keys are needed while
# we're on free tiers. Swap the strings below for direct provider plugins
# later if we outgrow LiveKit Inference's included usage.
STT_MODEL = "deepgram/nova-3:en"
LLM_MODEL = "openai/gpt-4.1-mini"
TTS_MODEL = "cartesia/sonic-3:6f84f4b8-58a2-430c-8c79-688dad597532"


async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect()

    session = AgentSession(
        vad=silero.VAD.load(),
        stt=STT_MODEL,
        llm=LLM_MODEL,
        tts=TTS_MODEL,
    )

    agent = Agent(instructions="You are VoxFlow, a helpful, concise voice assistant.")

    await session.start(agent=agent, room=ctx.room)


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))
