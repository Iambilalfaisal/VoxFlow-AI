"""OpenTelemetry tracing setup, shared by the API, worker, and history-writer.

livekit-agents ships its own internal tracer (livekit.agents.telemetry) that
already spans every STT/LLM/TTS request - it's built for LiveKit Cloud's
hosted observability product, but `set_tracer_provider` is the public seam it
exposes for an integrator's own OTel provider, and it adopts (not replaces)
whatever provider is already registered. Configuring our own provider here
and handing it to that seam means the SDK's own provider-call spans flow to
our local Jaeger too, instead of us re-instrumenting those calls by hand.
"""

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

from core.config import settings

_configured = False


def configure_tracing(service_name: str) -> None:
    """Idempotent per-process setup. Safe to call from module import time in
    the worker/history-writer entrypoints and from FastAPI startup."""
    global _configured
    if _configured or not settings.enable_observability:
        return
    _configured = True

    provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name}),
        sampler=ParentBased(TraceIdRatioBased(settings.trace_sample_ratio)),
    )
    exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_traces_endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    try:
        from livekit.agents.telemetry import set_tracer_provider as _lk_set_tracer_provider
    except ImportError:
        return  # not running inside a livekit-agents process (e.g. the API)
    _lk_set_tracer_provider(provider)


def instrument_fastapi(app) -> None:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)
