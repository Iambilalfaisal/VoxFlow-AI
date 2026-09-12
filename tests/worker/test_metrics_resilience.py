import types

from core.metrics import (
    PROVIDER_BREAKER_AVAILABLE,
    PROVIDER_ERRORS_TOTAL,
    PROVIDER_IN_FLIGHT,
    PROVIDER_LATENCY_SECONDS,
    PROVIDER_RATE_LIMITED_TOTAL,
    handle_availability_changed,
    handle_metrics_collected,
    handle_provider_error,
)
from worker.resilience import ProviderBudget


def _counter_value(counter, **labels) -> float:
    for metric in counter.collect():
        for sample in metric.samples:
            if sample.name.endswith("_total") and sample.labels == labels:
                return sample.value
    return 0.0


def _histogram_count(histogram, **labels) -> float:
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name.endswith("_count") and sample.labels == labels:
                return sample.value
    return 0.0


async def test_provider_budget_updates_in_flight_gauge():
    budget = ProviderBudget("test-in-flight", max_concurrent=2)

    async with budget:
        assert PROVIDER_IN_FLIGHT.labels(provider="test-in-flight")._value.get() == 1

    assert PROVIDER_IN_FLIGHT.labels(provider="test-in-flight")._value.get() == 0


def test_handle_metrics_collected_records_provider_latency():
    event = types.SimpleNamespace(
        metrics=types.SimpleNamespace(type="llm_metrics", duration=0.42)
    )
    before = _histogram_count(PROVIDER_LATENCY_SECONDS, provider="llm")

    handle_metrics_collected(event)

    assert _histogram_count(PROVIDER_LATENCY_SECONDS, provider="llm") == before + 1


def test_handle_metrics_collected_ignores_non_provider_metrics():
    event = types.SimpleNamespace(metrics=types.SimpleNamespace(type="vad_metrics"))
    # Must not raise for metric types with no `duration` field (e.g. VADMetrics).
    handle_metrics_collected(event)


class _FakeRateLimitError(Exception):
    status_code = 429


def test_handle_provider_error_increments_error_and_breaker_gauge_together():
    """Firing a fake provider error should move the error counter and the
    breaker-availability gauge together - the same event source
    (agent.py's session listeners) drives both."""
    event = types.SimpleNamespace(
        error=types.SimpleNamespace(type="stt_error", error=_FakeRateLimitError())
    )
    errors_before = _counter_value(PROVIDER_ERRORS_TOTAL, provider="stt")
    rate_limited_before = _counter_value(PROVIDER_RATE_LIMITED_TOTAL, provider="stt")

    handle_provider_error(event)
    handle_availability_changed("stt", available=False)

    assert _counter_value(PROVIDER_ERRORS_TOTAL, provider="stt") == errors_before + 1
    assert _counter_value(PROVIDER_RATE_LIMITED_TOTAL, provider="stt") == rate_limited_before + 1
    assert PROVIDER_BREAKER_AVAILABLE.labels(provider="stt")._value.get() == 0

    handle_availability_changed("stt", available=True)
    assert PROVIDER_BREAKER_AVAILABLE.labels(provider="stt")._value.get() == 1


def test_handle_provider_error_without_status_code_skips_rate_limit_counter():
    event = types.SimpleNamespace(
        error=types.SimpleNamespace(type="llm_error", error=RuntimeError("connection reset"))
    )
    rate_limited_before = _counter_value(PROVIDER_RATE_LIMITED_TOTAL, provider="llm")

    handle_provider_error(event)

    assert _counter_value(PROVIDER_RATE_LIMITED_TOTAL, provider="llm") == rate_limited_before
