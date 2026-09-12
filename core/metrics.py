"""Prometheus metrics shared by the API, worker, and history-writer processes.

Provider latency/error metrics are fed from livekit-agents' own
`metrics_collected`/`error` session events (see worker/agent.py) rather than
wrapping each STT/LLM/TTS call ourselves - the SDK already measures every
request internally (duration, ttft/ttfb, per-provider `label`), so hooking
that is "reuse, don't duplicate" in the same spirit as agent.py already
follows for the FallbackAdapter circuit breaker.

The worker runs every job in its own OS subprocess (LiveKit Agents' process
pool, not threads) - each spawned subprocess re-imports this module, so
in-memory Counter/Gauge/Histogram state is per-subprocess and would vanish
when the job's process exits. `PROMETHEUS_MULTIPROC_DIR` (set in
docker-compose.yml for the worker service only) switches prometheus_client
into its standard multiprocess mode: every process writes to mmap'd files in
that directory instead of memory, and whichever process actually serves
/metrics aggregates across all of them via MultiProcessCollector. This is
the same mechanism prometheus_client documents for prefork servers
(gunicorn, etc.) - this worker's job-per-subprocess model is that same shape.
"""

import atexit
import os

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, make_asgi_app
from prometheus_client import multiprocess
from prometheus_client.exposition import start_http_server

from core.config import settings

_MULTIPROC_DIR = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
_MULTIPROC = bool(_MULTIPROC_DIR)

if _MULTIPROC:
    # prometheus_client writes mmap'd value files here per-process; it does
    # not create the directory itself.
    os.makedirs(_MULTIPROC_DIR, exist_ok=True)
    # Every process (main + each spawned job) must clean up its own value
    # files on exit, or MultiProcessCollector keeps reading a dead process's
    # last-known values forever.
    atexit.register(multiprocess.mark_process_dead, os.getpid())

# --- Provider metrics (the quota-ceiling early warning) ---------------------

PROVIDER_LATENCY_SECONDS = Histogram(
    "voxflow_provider_latency_seconds",
    "Provider call duration as reported by livekit-agents' own metrics_collected event",
    ["provider"],
)
PROVIDER_ERRORS_TOTAL = Counter(
    "voxflow_provider_errors_total",
    "Provider call errors",
    ["provider"],
)
PROVIDER_RATE_LIMITED_TOTAL = Counter(
    "voxflow_provider_rate_limited_total",
    "Provider calls that failed with a 429/rate-limit status",
    ["provider"],
)
PROVIDER_IN_FLIGHT = Gauge(
    "voxflow_provider_in_flight",
    "Current in-flight calls held by a worker/resilience.py ProviderBudget",
    ["provider"],
    multiprocess_mode="livesum",  # total across every live job subprocess
)
PROVIDER_BREAKER_AVAILABLE = Gauge(
    "voxflow_provider_breaker_available",
    "1 if the provider's FallbackAdapter reports it available, 0 if unavailable",
    ["provider"],
    multiprocess_mode="min",  # 0 (degraded) if any job subprocess reports unavailable
)

# --- Write-path metrics (the write-path-saturation early warning) -----------

HISTORY_STREAM_DEPTH = Gauge(
    "voxflow_history_stream_depth", "XLEN of the history-events Redis stream", ["stream"]
)
HISTORY_DLQ_DEPTH = Gauge(
    "voxflow_history_dlq_depth", "XLEN of the history-events dead-letter stream", ["stream"]
)
HISTORY_STREAM_PENDING = Gauge(
    "voxflow_history_stream_pending",
    "Pending (delivered but unacked) entries per XPENDING",
    ["stream"],
)
HISTORY_WRITER_BATCH_LATENCY_SECONDS = Histogram(
    "voxflow_history_writer_batch_latency_seconds", "Batched insert transaction duration"
)
HISTORY_WRITER_ROWS_TOTAL = Counter(
    "voxflow_history_writer_rows_total", "Message rows successfully inserted"
)
HISTORY_WRITER_DEAD_LETTERED_TOTAL = Counter(
    "voxflow_history_writer_dead_lettered_total", "Stream entries dead-lettered"
)

# --- System metrics -----------------------------------------------------------

ACTIVE_SESSIONS = Gauge(
    "voxflow_active_sessions",
    "Voice sessions currently active on this worker",
    multiprocess_mode="livesum",  # one job subprocess per session; sum the live ones
)


def handle_metrics_collected(event) -> None:
    """`AgentSession.on("metrics_collected", ...)` handler.

    `event.metrics` is one of livekit.agents.metrics's STTMetrics/LLMMetrics/
    TTSMetrics/... union (discriminated by `.type`); only the three provider
    stages have a per-request `duration` we care about here.
    """
    provider = {"stt_metrics": "stt", "llm_metrics": "llm", "tts_metrics": "tts"}.get(
        event.metrics.type
    )
    if provider is None:
        return
    PROVIDER_LATENCY_SECONDS.labels(provider=provider).observe(event.metrics.duration)


def handle_provider_error(event) -> None:
    """`AgentSession.on("error", ...)` handler.

    `event.error` is one of LLMError/STTError/TTSError/... (discriminated by
    `.type`), wrapping the raised exception on `.error`. A 429/rate-limit is
    surfaced as an `APIStatusError` with `status_code == 429`.
    """
    provider = {"stt_error": "stt", "llm_error": "llm", "tts_error": "tts"}.get(event.error.type)
    if provider is None:
        return
    PROVIDER_ERRORS_TOTAL.labels(provider=provider).inc()
    if getattr(event.error.error, "status_code", None) == 429:
        PROVIDER_RATE_LIMITED_TOTAL.labels(provider=provider).inc()


def handle_availability_changed(provider: str, available: bool) -> None:
    """Companion to agent.py's `_log_availability` - same event, additive gauge."""
    PROVIDER_BREAKER_AVAILABLE.labels(provider=provider).set(1 if available else 0)


def _multiprocess_registry() -> CollectorRegistry:
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return registry


def start_metrics_server(port: int) -> None:
    """For the worker/history-writer processes, which aren't ASGI apps.

    Call this from exactly one process - in the worker's case, the main
    process (see worker/agent.py), never from a spawned job subprocess:
    every job subprocess re-imports this module and would otherwise try to
    rebind the same port. Job subprocesses still need no explicit "start" to
    contribute metrics - in multiprocess mode, writing to a Counter/Gauge/
    Histogram writes straight to their own value file.
    """
    if not settings.enable_observability:
        return
    if _MULTIPROC:
        start_http_server(port, registry=_multiprocess_registry())
    else:
        start_http_server(port)


def metrics_asgi_app():
    """For FastAPI: `app.mount("/metrics", metrics_asgi_app())`."""
    if _MULTIPROC:
        return make_asgi_app(_multiprocess_registry())
    return make_asgi_app()
