"""Truthful HTTP client for the Streamhouse demo-app REST API.

Adheres strictly to CONTRIBUTING.md:
- Ground Rule 1: Never fakes a pipeline step. Every status, offset, job ID,
  read-back verification, lookup row, SQL row, and Python stdout/stderr comes
  from the live demo-app API backed by Kafka, Flink, Fluss, and python-runner.
  Failures and timeouts raise exceptions or surface exact server errors.
- Ground Rule 2: Stages 1-4 are separate explicit operations triggered via
  ``POST /api/stages/{1..4}`` and polled via ``GET /api/state`` until terminal.
- Ground Rule 3: Stage 3 succeeds only when Fluss native read-back confirms
  every expected row (``read_back.verified is True`` and ``matched == checked > 0``).
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from opentelemetry.trace import SpanKind, Status, StatusCode

from shared.telemetry import (
    get_tracer,
    inject_trace_request_hook,
    set_span_attributes,
)

DEFAULT_APP_URL = "http://localhost:8090"
DEFAULT_FLINK_URL = "http://localhost:8081"
DEFAULT_SQL_GATEWAY_URL = "http://localhost:8083"

VALID_LOOKUP_TABLES: dict[str, tuple[str, ...]] = {
    "supplier_risk": ("run_id", "supplier_id"),
    "payment_reviews": ("run_id", "payment_id"),
    "event_receipts": ("run_id", "event_id"),
}

SQL_TERMINAL_STATES = frozenset({"FINISHED", "FAILED", "CANCELED"})
PYTHON_TERMINAL_STATES = frozenset({"FINISHED", "FAILED", "TIMED_OUT", "CANCELED"})


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class StreamhousePipelineError(RuntimeError):
    """Base error raised when an interaction with the Streamhouse stack fails."""

    def __init__(self, message: str, *, timestamp: str | None = None) -> None:
        super().__init__(message)
        self.timestamp: str = timestamp or _utc_now_iso()


class ServiceUnavailableError(StreamhousePipelineError):
    """Raised when demo-app or a downstream service (Kafka, Flink, Fluss) is unreachable."""


class StageConflictError(StreamhousePipelineError):
    """Raised on HTTP 409 when a precondition is unmet or another operation is running."""


class StageExecutionError(StreamhousePipelineError):
    """Raised when an operation fails, is rejected, times out, or fails read-back verification."""


def _extract_error_message(resp: httpx.Response) -> str:
    """Extract the server's JSON ``error`` (and optional ``record_errors``) when available."""
    try:
        payload = resp.json()
        if isinstance(payload, dict) and payload.get("error"):
            msg = str(payload["error"])
            record_errors = payload.get("record_errors")
            if isinstance(record_errors, list) and record_errors:
                details = []
                for item in record_errors:
                    idx = item.get("index")
                    errs = "; ".join(str(e) for e in (item.get("errors") or []))
                    prefix = f"record[{idx}]: " if idx is not None else ""
                    if errs:
                        details.append(f"{prefix}{errs}")
                if details:
                    msg = f"{msg} ({' | '.join(details)})"
            return msg
    except ValueError:
        pass
    text = resp.text.strip()
    return text or f"HTTP {resp.status_code}"


def _auth_headers(access_token: str | None) -> dict[str, str]:
    token = (access_token or os.environ.get("PLAYGROUND_ACCESS_TOKEN") or "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


class StreamhouseClient:
    """Async HTTP client talking directly to ``demo-app`` (default ``http://localhost:8090``)."""

    def __init__(
        self,
        app_url: str | None = None,
        flink_url: str = DEFAULT_FLINK_URL,
        sql_gateway_url: str = DEFAULT_SQL_GATEWAY_URL,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        resolved_app = app_url or os.environ.get("STREAMHOUSE_APP_URL", DEFAULT_APP_URL)
        self.app_url = resolved_app.rstrip("/")
        self.flink_url = flink_url.rstrip("/")
        self.sql_gateway_url = sql_gateway_url.rstrip("/")
        self._transport = transport

    def _http_client(self, timeout: float = 10.0) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=timeout,
            transport=self._transport,
            event_hooks={"request": [inject_trace_request_hook]},
        )

    # ------------------------------------------------------------------ state & runs

    async def get_state(self, *, timeout: float = 10.0) -> dict[str, Any]:
        """Fetch the live ``GET /api/state`` snapshot from demo-app."""
        url = f"{self.app_url}/api/state"
        async with self._http_client(timeout=timeout) as client:
            try:
                resp = await client.get(url)
            except httpx.HTTPError as exc:
                raise ServiceUnavailableError(
                    f"Unable to reach demo-app at {url}: {exc}"
                ) from exc

        if resp.status_code != 200:
            raise StreamhousePipelineError(
                f"GET /api/state failed (HTTP {resp.status_code}): {_extract_error_message(resp)}"
            )
        return resp.json()

    async def get_health(self, *, timeout: float = 10.0) -> dict[str, Any]:
        """Return the real component health snapshot reported by demo-app."""
        state = await self.get_state(timeout=timeout)
        health = state.get("health")
        if not isinstance(health, dict):
            raise StreamhousePipelineError(
                "demo-app /api/state response is missing 'health'."
            )
        return health

    async def start_new_run(self, *, timeout: float = 10.0) -> dict[str, Any]:
        """Start a fresh demo run via ``POST /api/runs`` (returns ``{'run_id': ...}``)."""
        url = f"{self.app_url}/api/runs"
        with get_tracer().start_as_current_span(
            "streamhouse.start_new_run", kind=SpanKind.CLIENT
        ) as span:
            set_span_attributes(span, {"http.method": "POST", "url.full": url})
            async with self._http_client(timeout=timeout) as client:
                try:
                    resp = await client.post(url, json={})
                except httpx.HTTPError as exc:
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    raise ServiceUnavailableError(
                        f"Unable to reach demo-app at {url}: {exc}"
                    ) from exc

            span.set_attribute("http.response.status_code", resp.status_code)
            if resp.status_code == 409:
                msg = _extract_error_message(resp)
                span.set_status(Status(StatusCode.ERROR, msg))
                raise StageConflictError(msg)
            if resp.status_code not in (200, 201):
                msg = (
                    f"POST /api/runs failed (HTTP {resp.status_code}): "
                    f"{_extract_error_message(resp)}"
                )
                span.set_status(Status(StatusCode.ERROR, msg))
                raise StreamhousePipelineError(msg)
            data = resp.json()
            set_span_attributes(span, {"streamhouse.run_id": data.get("run_id")})
            return data

    # ------------------------------------------------------------------ stages 1-4

    async def _trigger_stage(
        self, stage: int, *, timeout: float = 15.0
    ) -> dict[str, Any]:
        """POST ``/api/stages/{stage}`` and validate the 202 Accepted handshake."""
        if stage not in (1, 2, 3, 4):
            raise ValueError(f"stage must be 1..4, got {stage}")
        url = f"{self.app_url}/api/stages/{stage}"
        async with self._http_client(timeout=timeout) as client:
            try:
                resp = await client.post(url, json={})
            except httpx.HTTPError as exc:
                raise ServiceUnavailableError(
                    f"Unable to reach demo-app at {url}: {exc}"
                ) from exc

        if resp.status_code == 409:
            raise StageConflictError(_extract_error_message(resp))
        if resp.status_code == 503:
            raise ServiceUnavailableError(_extract_error_message(resp))
        if resp.status_code not in (200, 202):
            raise StreamhousePipelineError(
                f"Stage {stage} trigger failed (HTTP {resp.status_code}): "
                f"{_extract_error_message(resp)}"
            )
        return resp.json()

    async def wait_for_stage(
        self,
        stage: int,
        *,
        expected_run_id: str | None = None,
        poll_interval: float = 0.25,
        timeout: float = 120.0,
    ) -> tuple[str, dict[str, Any]]:
        """Poll ``GET /api/state`` until ``run.stage{stage}`` reaches SUCCEEDED or FAILED."""
        stage_key = f"stage{stage}"
        deadline = time.monotonic() + timeout
        last_phase: str | None = None
        last_status: str | None = None

        while True:
            state = await self.get_state(timeout=min(10.0, max(2.0, timeout)))
            run = state.get("run")
            if not isinstance(run, dict):
                raise StageExecutionError(
                    f"Stage {stage} polling failed: demo-app reports no active run."
                )

            run_id = str(run.get("run_id") or "")
            if expected_run_id and run_id != expected_run_id:
                raise StageExecutionError(
                    f"Active run changed while awaiting stage {stage}: "
                    f"expected {expected_run_id}, observed {run_id}."
                )

            stage_state = run.get(stage_key)
            if not isinstance(stage_state, dict):
                raise StageExecutionError(
                    f"demo-app /api/state response is missing 'run.{stage_key}'."
                )

            status = stage_state.get("status")
            last_status = str(status) if status is not None else None
            last_phase = stage_state.get("phase")

            if status == "SUCCEEDED":
                return run_id, stage_state
            if status == "FAILED":
                err = (
                    stage_state.get("error")
                    or f"Stage {stage} failed without an error message."
                )
                detail = stage_state.get("error_detail")
                msg = f"{err} ({detail})" if detail else str(err)
                raise StageExecutionError(
                    msg,
                    timestamp=stage_state.get("finished_at")
                    or stage_state.get("started_at"),
                )

            if time.monotonic() >= deadline:
                phase_note = f", last phase: {last_phase}" if last_phase else ""
                raise StageExecutionError(
                    f"Stage {stage} timed out after {timeout:.1f}s "
                    f"(last status: {last_status}{phase_note})."
                )

            await asyncio.sleep(poll_interval)

    async def emit_events(
        self,
        *,
        poll_interval: float = 0.25,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        """Stage 1: Trigger Kafka scenario emission and wait for real broker acknowledgments."""
        with get_tracer().start_as_current_span(
            "streamhouse.stage1.emit_events", kind=SpanKind.CLIENT
        ) as span:
            span.set_attribute("streamhouse.stage", 1)
            try:
                accepted = await self._trigger_stage(1)
                run_id, stage1 = await self.wait_for_stage(
                    1,
                    expected_run_id=accepted.get("run_id"),
                    poll_interval=poll_interval,
                    timeout=timeout,
                )
                events = stage1.get("events")
                if not isinstance(events, list) or not events:
                    raise StageExecutionError(
                        "Stage 1 reported SUCCEEDED, but demo-app returned no acknowledged Kafka events."
                    )
                unacked = [
                    ev.get("event_id", "<unknown>")
                    for ev in events
                    if ev.get("status") != "ACKNOWLEDGED"
                    or ev.get("partition") is None
                    or ev.get("offset") is None
                ]
                if unacked:
                    raise StageExecutionError(
                        f"Stage 1 returned events without complete Kafka acknowledgments: {unacked}"
                    )
                set_span_attributes(
                    span,
                    {
                        "streamhouse.run_id": run_id,
                        "messaging.system": "kafka",
                        "messaging.destination.name": stage1.get("topic"),
                        "streamhouse.events.count": len(events),
                        "streamhouse.duration_ms": stage1.get("duration_ms"),
                    },
                )
                return {
                    "run_id": run_id,
                    "stage": 1,
                    "status": stage1["status"],
                    "topic": stage1.get("topic"),
                    "duration_ms": stage1.get("duration_ms"),
                    "steps": stage1.get("steps", []),
                    "events": events,
                }
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    async def run_processing_job(
        self,
        *,
        poll_interval: float = 0.25,
        timeout: float = 120.0,
    ) -> dict[str, Any]:
        """Stage 2: Submit bounded Flink processing job and wait for FINISHED + computed results."""
        with get_tracer().start_as_current_span(
            "streamhouse.stage2.flink_processing", kind=SpanKind.CLIENT
        ) as span:
            span.set_attribute("streamhouse.stage", 2)
            try:
                accepted = await self._trigger_stage(2)
                run_id, stage2 = await self.wait_for_stage(
                    2,
                    expected_run_id=accepted.get("run_id"),
                    poll_interval=poll_interval,
                    timeout=timeout,
                )
                execution = stage2.get("execution")
                if not isinstance(execution, dict):
                    raise StageExecutionError(
                        "Stage 2 reported SUCCEEDED, but demo-app returned no Flink execution record."
                    )
                if execution.get("job_state") != "FINISHED":
                    raise StageExecutionError(
                        f"Stage 2 Flink job did not reach FINISHED (observed: {execution.get('job_state')})."
                    )
                results = execution.get("results")
                if not isinstance(results, list) or not results:
                    raise StageExecutionError(
                        "Stage 2 Flink job finished, but no computed results were read back from Kafka."
                    )
                set_span_attributes(
                    span,
                    {
                        "streamhouse.run_id": run_id,
                        "streamhouse.flink.job_id": execution.get("job_id"),
                        "streamhouse.flink.execution_id": execution.get("execution_id"),
                        "streamhouse.results.count": len(results),
                        "streamhouse.duration_ms": stage2.get("duration_ms"),
                    },
                )
                return {
                    "run_id": run_id,
                    "stage": 2,
                    "status": stage2["status"],
                    "duration_ms": stage2.get("duration_ms"),
                    "steps": stage2.get("steps", []),
                    "execution": execution,
                }
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    async def run_ingestion_job(
        self,
        *,
        poll_interval: float = 0.25,
        timeout: float = 120.0,
    ) -> dict[str, Any]:
        """Stage 3: Submit bounded Flink ingestion job and require native Fluss read-back proof."""
        with get_tracer().start_as_current_span(
            "streamhouse.stage3.fluss_ingestion", kind=SpanKind.CLIENT
        ) as span:
            span.set_attribute("streamhouse.stage", 3)
            try:
                accepted = await self._trigger_stage(3)
                run_id, stage3 = await self.wait_for_stage(
                    3,
                    expected_run_id=accepted.get("run_id"),
                    poll_interval=poll_interval,
                    timeout=timeout,
                )
                execution = stage3.get("execution")
                if not isinstance(execution, dict):
                    raise StageExecutionError(
                        "Stage 3 reported SUCCEEDED, but demo-app returned no ingestion execution record."
                    )
                if execution.get("job_state") != "FINISHED":
                    raise StageExecutionError(
                        f"Stage 3 ingestion job did not reach FINISHED (observed: {execution.get('job_state')})."
                    )
                read_back = execution.get("read_back")
                if not isinstance(read_back, dict):
                    raise StageExecutionError(
                        "Stage 3 missing native Fluss read_back verification payload."
                    )
                checked = read_back.get("checked", 0)
                matched = read_back.get("matched", 0)
                if not read_back.get("verified") or checked <= 0 or matched != checked:
                    raise StageExecutionError(
                        f"Stage 3 persistence not proven by Fluss read-back: "
                        f"verified={read_back.get('verified')}, matched={matched}/{checked}."
                    )
                set_span_attributes(
                    span,
                    {
                        "streamhouse.run_id": run_id,
                        "streamhouse.flink.job_id": execution.get("job_id"),
                        "streamhouse.fluss.checked": checked,
                        "streamhouse.fluss.matched": matched,
                        "streamhouse.duration_ms": stage3.get("duration_ms"),
                    },
                )
                return {
                    "run_id": run_id,
                    "stage": 3,
                    "status": stage3["status"],
                    "duration_ms": stage3.get("duration_ms"),
                    "warning": stage3.get("warning"),
                    "steps": stage3.get("steps", []),
                    "execution": execution,
                }
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    async def query_fluss_stage(
        self,
        *,
        poll_interval: float = 0.25,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Stage 4: Trigger native Fluss primary-key lookups for supplier_risk and payment_reviews."""
        with get_tracer().start_as_current_span(
            "streamhouse.stage4.fluss_query", kind=SpanKind.CLIENT
        ) as span:
            span.set_attribute("streamhouse.stage", 4)
            try:
                accepted = await self._trigger_stage(4)
                run_id, stage4 = await self.wait_for_stage(
                    4,
                    expected_run_id=accepted.get("run_id"),
                    poll_interval=poll_interval,
                    timeout=timeout,
                )
                result = stage4.get("result")
                if not isinstance(result, dict):
                    raise StageExecutionError(
                        "Stage 4 reported SUCCEEDED, but demo-app returned no Fluss query result."
                    )
                supplier = result.get("supplier") or {}
                review = result.get("review") or {}
                if not (supplier.get("ok") and supplier.get("found")):
                    raise StageExecutionError(
                        f"Stage 4 supplier_risk lookup did not return a row: {supplier.get('error') or 'not found'}"
                    )
                if not (review.get("ok") and review.get("found")):
                    raise StageExecutionError(
                        f"Stage 4 payment_reviews lookup did not return a row: {review.get('error') or 'not found'}"
                    )
                set_span_attributes(
                    span,
                    {
                        "streamhouse.run_id": run_id,
                        "streamhouse.duration_ms": stage4.get("duration_ms"),
                    },
                )
                return {
                    "run_id": run_id,
                    "stage": 4,
                    "status": stage4["status"],
                    "duration_ms": stage4.get("duration_ms"),
                    "steps": stage4.get("steps", []),
                    "result": result,
                }
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    # ------------------------------------------------------------------ native lookups

    async def lookup_row(
        self,
        table: str,
        *,
        timeout: float = 10.0,
        **primary_key: str,
    ) -> dict[str, Any]:
        """Execute a direct native Fluss primary-key lookup via ``GET /api/lookup/{table}``."""
        if table not in VALID_LOOKUP_TABLES:
            valid = ", ".join(VALID_LOOKUP_TABLES)
            raise ValueError(f"Unknown Fluss table '{table}'; expected one of: {valid}")
        required = VALID_LOOKUP_TABLES[table]
        missing = [col for col in required if not primary_key.get(col)]
        if missing:
            raise ValueError(
                f"Lookup for '{table}' requires primary key fields: {missing}"
            )

        params = {col: str(primary_key[col]) for col in required}
        url = f"{self.app_url}/api/lookup/{table}"
        with get_tracer().start_as_current_span(
            f"streamhouse.lookup.{table}", kind=SpanKind.CLIENT
        ) as span:
            set_span_attributes(
                span,
                {
                    "streamhouse.fluss.table": table,
                    "streamhouse.run_id": params.get("run_id"),
                    "http.method": "GET",
                    "url.full": url,
                },
            )
            async with self._http_client(timeout=timeout) as client:
                try:
                    resp = await client.get(url, params=params)
                except httpx.HTTPError as exc:
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    raise ServiceUnavailableError(
                        f"Unable to reach demo-app at {url}: {exc}"
                    ) from exc

            span.set_attribute("http.response.status_code", resp.status_code)
            if resp.status_code == 200:
                obs = resp.json()
                if not obs.get("ok"):
                    msg = f"Fluss lookup for {table} failed: {obs.get('error') or 'unknown error'}"
                    span.set_status(Status(StatusCode.ERROR, msg))
                    raise ServiceUnavailableError(msg)
                set_span_attributes(
                    span,
                    {
                        "streamhouse.fluss.found": obs.get("found"),
                        "streamhouse.fluss.elapsed_ms": obs.get("elapsed_ms"),
                    },
                )
                return obs

            if resp.status_code == 404:
                try:
                    obs = resp.json()
                except ValueError:
                    obs = None
                if (
                    isinstance(obs, dict)
                    and obs.get("ok") is True
                    and obs.get("found") is False
                ):
                    span.set_attribute("streamhouse.fluss.found", False)
                    return obs
                msg = (
                    f"Fluss lookup for {table} failed (HTTP 404): "
                    f"{_extract_error_message(resp)}"
                )
                span.set_status(Status(StatusCode.ERROR, msg))
                raise StreamhousePipelineError(msg)

            if resp.status_code == 503:
                try:
                    obs = resp.json()
                    detail = obs.get("error") if isinstance(obs, dict) else None
                except ValueError:
                    detail = None
                msg = (
                    f"Fluss lookup unavailable for {table}: "
                    f"{detail or _extract_error_message(resp)}"
                )
                span.set_status(Status(StatusCode.ERROR, msg))
                raise ServiceUnavailableError(msg)

            msg = (
                f"Fluss lookup for {table} failed (HTTP {resp.status_code}): "
                f"{_extract_error_message(resp)}"
            )
            span.set_status(Status(StatusCode.ERROR, msg))
            raise StreamhousePipelineError(msg)

    async def _resolve_run_id(self, run_id: str | None) -> str:
        if run_id:
            return run_id
        state = await self.get_state()
        current_run = (state.get("run") or {}).get("run_id")
        if not current_run:
            raise StreamhousePipelineError(
                "No active run_id found in demo-app /api/state; start a run first."
            )
        return str(current_run)

    async def lookup_supplier(
        self,
        supplier_id: str = "SUP-NORTHSTAR",
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Native point lookup against ``payment_risk.supplier_risk``."""
        resolved_run = await self._resolve_run_id(run_id)
        return await self.lookup_row(
            "supplier_risk",
            run_id=resolved_run,
            supplier_id=supplier_id,
        )

    async def lookup_payment_review(
        self,
        payment_id: str = "PAY-NS-0001",
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Native point lookup against ``payment_risk.payment_reviews``."""
        resolved_run = await self._resolve_run_id(run_id)
        return await self.lookup_row(
            "payment_reviews",
            run_id=resolved_run,
            payment_id=payment_id,
        )

    # ------------------------------------------------------------------ interactive workspaces

    async def get_event_templates(
        self, *, timeout: float = 10.0
    ) -> list[dict[str, Any]]:
        """Fetch live manual-event templates for the active run (``GET /api/events/templates``)."""
        url = f"{self.app_url}/api/events/templates"
        async with self._http_client(timeout=timeout) as client:
            try:
                resp = await client.get(url)
            except httpx.HTTPError as exc:
                raise ServiceUnavailableError(
                    f"Unable to reach demo-app at {url}: {exc}"
                ) from exc
        if resp.status_code != 200:
            raise StreamhousePipelineError(
                f"GET /api/events/templates failed (HTTP {resp.status_code}): "
                f"{_extract_error_message(resp)}"
            )
        return resp.json()

    async def send_custom_events(
        self,
        topic: str,
        payload: str,
        *,
        key: str | None = None,
        batch: bool = False,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Produce user-entered JSON record(s) to Kafka via ``POST /api/events``."""
        if not payload or not payload.strip():
            raise StageExecutionError("The JSON payload is empty.")
        url = f"{self.app_url}/api/events"
        body = {
            "topic": topic,
            "key": key.strip() if key and key.strip() else None,
            "payload": payload,
            "batch": batch,
        }
        async with self._http_client(timeout=timeout) as client:
            try:
                resp = await client.post(url, json=body)
            except httpx.HTTPError as exc:
                raise ServiceUnavailableError(
                    f"Unable to reach demo-app at {url}: {exc}"
                ) from exc

        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 400:
            raise StageExecutionError(_extract_error_message(resp))
        if resp.status_code in (502, 503):
            raise ServiceUnavailableError(_extract_error_message(resp))
        raise StreamhousePipelineError(
            f"POST /api/events failed (HTTP {resp.status_code}): {_extract_error_message(resp)}"
        )

    async def get_sql_examples(self, *, timeout: float = 10.0) -> list[dict[str, Any]]:
        """Fetch live Flink SQL examples for the active run (``GET /api/sql/examples``)."""
        url = f"{self.app_url}/api/sql/examples"
        async with self._http_client(timeout=timeout) as client:
            try:
                resp = await client.get(url)
            except httpx.HTTPError as exc:
                raise ServiceUnavailableError(
                    f"Unable to reach demo-app at {url}: {exc}"
                ) from exc
        if resp.status_code != 200:
            raise StreamhousePipelineError(
                f"GET /api/sql/examples failed (HTTP {resp.status_code}): "
                f"{_extract_error_message(resp)}"
            )
        return resp.json()

    async def run_sql(
        self,
        statement: str,
        *,
        mode: str = "batch",
        workspace: str = "sql",
        access_token: str | None = None,
        poll_interval: float = 0.25,
        timeout: float = 120.0,
    ) -> dict[str, Any]:
        """Submit Flink SQL via ``POST /api/sql/executions`` and poll ``/api/state`` until done."""
        if not statement or not statement.strip():
            raise StageExecutionError("The SQL statement is empty.")
        url = f"{self.app_url}/api/sql/executions"
        headers = _auth_headers(access_token)
        body = {"statement": statement, "mode": mode, "workspace": workspace}
        async with self._http_client(timeout=15.0) as client:
            try:
                resp = await client.post(url, json=body, headers=headers)
            except httpx.HTTPError as exc:
                raise ServiceUnavailableError(
                    f"Unable to reach demo-app at {url}: {exc}"
                ) from exc

        if resp.status_code == 400:
            raise StageExecutionError(_extract_error_message(resp))
        if resp.status_code == 401:
            raise StreamhousePipelineError(_extract_error_message(resp))
        if resp.status_code == 409:
            raise StageConflictError(_extract_error_message(resp))
        if resp.status_code not in (200, 202):
            raise StreamhousePipelineError(
                f"SQL submission failed (HTTP {resp.status_code}): {_extract_error_message(resp)}"
            )

        execution_id = resp.json().get("execution_id")
        if not execution_id:
            raise StageExecutionError(
                "demo-app accepted SQL submission without an execution_id."
            )

        deadline = time.monotonic() + timeout
        last_status: str | None = None
        last_phase: str | None = None

        while True:
            state = await self.get_state(timeout=min(10.0, max(2.0, timeout)))
            executions = state.get("sql_executions") or []
            match = next((x for x in executions if x.get("id") == execution_id), None)
            if isinstance(match, dict):
                status = match.get("status")
                last_status = str(status) if status is not None else None
                last_phase = match.get("phase")
                if status in SQL_TERMINAL_STATES:
                    return match

            if time.monotonic() >= deadline:
                phase_note = f", last phase: {last_phase}" if last_phase else ""
                raise StageExecutionError(
                    f"SQL execution '{execution_id}' timed out after {timeout:.1f}s "
                    f"(last status: {last_status}{phase_note})."
                )
            await asyncio.sleep(poll_interval)

    async def get_python_example(self, *, timeout: float = 10.0) -> str:
        """Fetch live ``pyfluss`` example code for the active run (``GET /api/python/example``)."""
        url = f"{self.app_url}/api/python/example"
        async with self._http_client(timeout=timeout) as client:
            try:
                resp = await client.get(url)
            except httpx.HTTPError as exc:
                raise ServiceUnavailableError(
                    f"Unable to reach demo-app at {url}: {exc}"
                ) from exc
        if resp.status_code != 200:
            raise StreamhousePipelineError(
                f"GET /api/python/example failed (HTTP {resp.status_code}): "
                f"{_extract_error_message(resp)}"
            )
        return str(resp.json().get("code") or "")

    async def run_python(
        self,
        code: str,
        *,
        access_token: str | None = None,
        poll_interval: float = 0.25,
        timeout: float = 60.0,
    ) -> dict[str, Any]:
        """Submit Python (``pyfluss``) code via ``POST /api/python/executions`` and poll until done."""
        if not code or not code.strip():
            raise StageExecutionError("The Python code is empty.")
        url = f"{self.app_url}/api/python/executions"
        headers = _auth_headers(access_token)
        async with self._http_client(timeout=15.0) as client:
            try:
                resp = await client.post(url, json={"code": code}, headers=headers)
            except httpx.HTTPError as exc:
                raise ServiceUnavailableError(
                    f"Unable to reach demo-app at {url}: {exc}"
                ) from exc

        if resp.status_code == 400:
            raise StageExecutionError(_extract_error_message(resp))
        if resp.status_code == 401:
            raise StreamhousePipelineError(_extract_error_message(resp))
        if resp.status_code == 409:
            raise StageConflictError(_extract_error_message(resp))
        if resp.status_code not in (200, 202):
            raise StreamhousePipelineError(
                f"Python submission failed (HTTP {resp.status_code}): {_extract_error_message(resp)}"
            )

        execution_id = resp.json().get("execution_id")
        if not execution_id:
            raise StageExecutionError(
                "demo-app accepted Python submission without an execution_id."
            )

        deadline = time.monotonic() + timeout
        last_status: str | None = None

        while True:
            state = await self.get_state(timeout=min(10.0, max(2.0, timeout)))
            executions = state.get("python_executions") or []
            match = next((x for x in executions if x.get("id") == execution_id), None)
            if isinstance(match, dict):
                status = match.get("status")
                last_status = str(status) if status is not None else None
                if status in PYTHON_TERMINAL_STATES:
                    return match

            if time.monotonic() >= deadline:
                raise StageExecutionError(
                    f"Python execution '{execution_id}' timed out after {timeout:.1f}s "
                    f"(last status: {last_status})."
                )
            await asyncio.sleep(poll_interval)
