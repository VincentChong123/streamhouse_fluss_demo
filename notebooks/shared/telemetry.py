"""Lightweight OpenTelemetry tracing helpers for Streamhouse notebooks.

Follows ``notebooks/docs/observability.md``:
- Service name defaults to ``streamhouse-notebooks`` (overridable via ``OTEL_SERVICE_NAME``).
- Exports OTLP/HTTP Protobuf when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set (e.g.
  ``http://127.0.0.1:4318`` for Aspire Dashboard).
- Stays a zero-overhead no-op when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is unset, so unit tests
  and offline notebook runs never fail or stall.
- Injects W3C ``traceparent`` / ``tracestate`` headers into outbound ``httpx`` requests so
  ``demo-app``, Kafka, and Flink join the same distributed trace.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

DEFAULT_SERVICE_NAME = "streamhouse-notebooks"
INSTRUMENTATION_SCOPE = "streamhouse.notebooks.client"

_INITIALIZED = False
_PROVIDER: TracerProvider | None = None


def init_telemetry(
    *,
    service_name: str | None = None,
    endpoint: str | None = None,
    force: bool = False,
) -> TracerProvider | None:
    """Configure OTLP/HTTP trace export if an endpoint is configured.

    Returns the active ``TracerProvider`` when configured, or ``None`` when tracing export
    is disabled (no endpoint set). Safe to call multiple times.
    """
    global _INITIALIZED, _PROVIDER
    if _INITIALIZED and not force:
        return _PROVIDER

    resolved_endpoint = (
        endpoint
        if endpoint is not None
        else os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    )
    if not resolved_endpoint:
        _INITIALIZED = True
        return None

    svc = (
        service_name
        or os.environ.get("OTEL_SERVICE_NAME", "").strip()
        or DEFAULT_SERVICE_NAME
    )
    traces_url = (
        resolved_endpoint
        if resolved_endpoint.rstrip("/").endswith("/v1/traces")
        else f"{resolved_endpoint.rstrip('/')}/v1/traces"
    )
    resource = Resource.create({"service.name": svc})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_url))
    )
    trace.set_tracer_provider(provider)
    _PROVIDER = provider
    _INITIALIZED = True
    return provider


def get_tracer(name: str = INSTRUMENTATION_SCOPE) -> trace.Tracer:
    """Return an OTel tracer, initializing OTLP export lazily if configured via env."""
    if not _INITIALIZED:
        init_telemetry()
    return trace.get_tracer(name)


def flush_telemetry(timeout_millis: int = 5_000) -> bool:
    """Force-flush pending spans to the OTLP collector (no-op if export is disabled)."""
    if _PROVIDER is not None:
        return bool(_PROVIDER.force_flush(timeout_millis))
    return True


def shutdown_telemetry() -> None:
    """Flush and shut down the configured TracerProvider, if any."""
    global _INITIALIZED, _PROVIDER
    if _PROVIDER is not None:
        _PROVIDER.shutdown()
        _PROVIDER = None
    _INITIALIZED = False


def current_trace_id() -> str | None:
    """Return the active span's 32-hex-char trace_id, or ``None`` if no valid span is active."""
    ctx = trace.get_current_span().get_span_context()
    if ctx and ctx.is_valid:
        return format(ctx.trace_id, "032x")
    return None


def inject_trace_headers(headers: dict[str, str] | None = None) -> dict[str, str]:
    """Return a copy of ``headers`` with W3C ``traceparent``/``tracestate`` injected."""
    carrier: dict[str, str] = dict(headers) if headers else {}
    propagate.inject(carrier)
    return carrier


async def inject_trace_request_hook(request: httpx.Request) -> None:
    """``httpx`` async request hook that injects W3C trace context into outbound headers."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    for k, v in carrier.items():
        if k not in request.headers:
            request.headers[k] = v


def set_span_attributes(span: trace.Span, attrs: dict[str, Any]) -> None:
    """Attach non-None scalar attributes to ``span`` safely."""
    for key, value in attrs.items():
        if value is not None:
            span.set_attribute(key, value)
