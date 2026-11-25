"""Unit tests for Stage 1 notebook OpenTelemetry instrumentation."""

from __future__ import annotations

import httpx
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from shared import client as client_mod
from shared import telemetry
from shared.client import StreamhouseClient


@pytest.fixture()
def memory_tracer(monkeypatch: pytest.MonkeyPatch) -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    def fake_get_tracer(name: str = telemetry.INSTRUMENTATION_SCOPE):
        return provider.get_tracer(name)

    monkeypatch.setattr(telemetry, "get_tracer", fake_get_tracer)
    monkeypatch.setattr(client_mod, "get_tracer", fake_get_tracer)
    yield exporter
    provider.shutdown()


@pytest.mark.asyncio
async def test_stage1_emit_creates_span_and_injects_traceparent(
    memory_tracer: InMemorySpanExporter,
) -> None:
    seen_headers: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(dict(request.headers))
        if request.url.path == "/api/stages/1" and request.method == "POST":
            return httpx.Response(
                202, json={"run_id": "run-test-1", "stage": 1, "status": "RUNNING"}
            )
        if request.url.path == "/api/state":
            return httpx.Response(
                200,
                json={
                    "run": {
                        "run_id": "run-test-1",
                        "stage1": {
                            "status": "SUCCEEDED",
                            "topic": "business-events",
                            "duration_ms": 42,
                            "events": [
                                {
                                    "event_id": "evt-1",
                                    "status": "ACKNOWLEDGED",
                                    "partition": 0,
                                    "offset": 10,
                                }
                            ],
                        },
                    }
                },
            )
        return httpx.Response(404)

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    res = await client.emit_events(poll_interval=0.01)

    assert res["run_id"] == "run-test-1"
    spans = memory_tracer.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "streamhouse.stage1.emit_events"
    assert span.attributes["streamhouse.stage"] == 1
    assert span.attributes["streamhouse.run_id"] == "run-test-1"
    assert span.attributes["messaging.destination.name"] == "business-events"
    assert span.attributes["streamhouse.events.count"] == 1

    # Verify W3C traceparent was injected on outbound HTTP requests and matches this span
    expected_tid = format(span.context.trace_id, "032x")
    expected_sid = format(span.context.span_id, "016x")
    assert seen_headers, "expected outbound HTTP requests"
    for hdrs in seen_headers:
        tp = hdrs.get("traceparent", "")
        assert tp.startswith(f"00-{expected_tid}-{expected_sid}-"), (
            f"unexpected traceparent: {tp!r}"
        )


@pytest.mark.asyncio
async def test_lookup_row_creates_span_and_records_found(
    memory_tracer: InMemorySpanExporter,
) -> None:
    seen_traceparent: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_traceparent.append(request.headers.get("traceparent", ""))
        return httpx.Response(
            200,
            json={
                "ok": True,
                "found": True,
                "elapsed_ms": 3,
                "row": {"supplier_id": "SUP-1"},
            },
        )

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    obs = await client.lookup_row(
        "supplier_risk", run_id="run-test-2", supplier_id="SUP-1"
    )

    assert obs["found"] is True
    spans = memory_tracer.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "streamhouse.lookup.supplier_risk"
    assert span.attributes["streamhouse.fluss.table"] == "supplier_risk"
    assert span.attributes["streamhouse.fluss.found"] is True
    assert seen_traceparent[0].startswith(
        f"00-{format(span.context.trace_id, '032x')}-"
    )


def test_init_telemetry_noop_without_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    telemetry.shutdown_telemetry()
    assert telemetry.init_telemetry(force=True) is None
    assert telemetry.flush_telemetry() is True
    telemetry.shutdown_telemetry()
