"""Offline unit and CONTRIBUTING.md compliance tests for the Marimo client & notebooks.

These tests never pretend to be integration tests: they verify fail-fast error
handling, async stage & workspace polling, Ground Rule 3 read-back enforcement,
exact ``demo-app`` REST URLs, and static notebook compliance without requiring Docker.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from shared.client import (
    ServiceUnavailableError,
    StageConflictError,
    StageExecutionError,
    StreamhouseClient,
    StreamhousePipelineError,
)
from shared.scenario import PAYMENT_ID, SUPPLIER_ID

NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent


def _json_response(status: int, payload: dict[str, Any]) -> httpx.Response:
    return httpx.Response(
        status_code=status,
        content=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
    )


def _sample_state(
    *,
    run_id: str = "run-20261123-100000-abcd",
    stage1: dict[str, Any] | None = None,
    stage2: dict[str, Any] | None = None,
    stage3: dict[str, Any] | None = None,
    stage4: dict[str, Any] | None = None,
    sql_executions: list[dict[str, Any]] | None = None,
    python_executions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "health": {"overall": "READY", "components": []},
        "run": {
            "run_id": run_id,
            "supplier_id": SUPPLIER_ID,
            "payment_id": PAYMENT_ID,
            "stage1": stage1 or {"status": "NOT_STARTED"},
            "stage2": stage2 or {"status": "NOT_STARTED"},
            "stage3": stage3 or {"status": "NOT_STARTED"},
            "stage4": stage4 or {"status": "NOT_STARTED"},
        },
        "sql_executions": sql_executions or [],
        "python_executions": python_executions or [],
    }


@pytest.mark.asyncio
async def test_fail_fast_on_connection_error():
    """Ground Rule 1: connection failures raise ServiceUnavailableError, never fake data."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused", request=request)

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    with pytest.raises(ServiceUnavailableError):
        await client.emit_events()
    with pytest.raises(ServiceUnavailableError):
        await client.run_processing_job()
    with pytest.raises(ServiceUnavailableError):
        await client.run_ingestion_job()
    with pytest.raises(ServiceUnavailableError):
        await client.query_fluss_stage()
    with pytest.raises(ServiceUnavailableError):
        await client.lookup_row(
            "supplier_risk", run_id="run-1", supplier_id=SUPPLIER_ID
        )
    with pytest.raises(ServiceUnavailableError):
        await client.send_custom_events("playground-events", '{"a": 1}')
    with pytest.raises(ServiceUnavailableError):
        await client.run_sql("SHOW TABLES")
    with pytest.raises(ServiceUnavailableError):
        await client.run_python("print(1)")


@pytest.mark.asyncio
async def test_stage1_polls_until_succeeded_and_returns_real_kafka_acks():
    polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.method == "POST" and request.url.path == "/api/stages/1":
            return _json_response(
                202, {"stage": 1, "run_id": "run-test-1", "status": "RUNNING"}
            )
        if request.method == "GET" and request.url.path == "/api/state":
            polls += 1
            if polls == 1:
                return _json_response(
                    200,
                    _sample_state(
                        run_id="run-test-1",
                        stage1={
                            "status": "RUNNING",
                            "phase": "Sending SUPPLIER_REGISTERED (1/4)",
                        },
                    ),
                )
            return _json_response(
                200,
                _sample_state(
                    run_id="run-test-1",
                    stage1={
                        "status": "SUCCEEDED",
                        "topic": "business-events",
                        "duration_ms": 480,
                        "steps": [{"name": "Sending", "ms": 480, "running": False}],
                        "events": [
                            {
                                "event_id": f"evt-{i}",
                                "event_type": t,
                                "occurred_at": f"2026-11-23T10:0{i}:00Z",
                                "key": f"run-test-1:{SUPPLIER_ID}",
                                "status": "ACKNOWLEDGED",
                                "partition": 0,
                                "offset": 10 + i,
                                "send_ms": 5,
                            }
                            for i, t in enumerate(
                                [
                                    "SUPPLIER_REGISTERED",
                                    "BENEFICIARY_CHANGED",
                                    "IDENTITY_ACTIVITY",
                                    "PAYMENT_REQUESTED",
                                ]
                            )
                        ],
                    },
                ),
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    res = await client.emit_events(poll_interval=0.01, timeout=2.0)
    assert polls == 2
    assert res["run_id"] == "run-test-1"
    assert res["status"] == "SUCCEEDED"
    assert [ev["offset"] for ev in res["events"]] == [10, 11, 12, 13]


@pytest.mark.asyncio
async def test_stage1_rejects_empty_or_unacked_events():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _json_response(
                202, {"stage": 1, "run_id": "run-1", "status": "RUNNING"}
            )
        return _json_response(
            200,
            _sample_state(
                run_id="run-1",
                stage1={
                    "status": "SUCCEEDED",
                    "topic": "business-events",
                    "events": [],
                },
            ),
        )

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    with pytest.raises(StageExecutionError, match="no acknowledged Kafka events"):
        await client.emit_events(poll_interval=0.01, timeout=1.0)


@pytest.mark.asyncio
async def test_stage_conflict_409_raises_stage_conflict_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return _json_response(
            409, {"error": "Requires stage 1: emit the scenario events first."}
        )

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    with pytest.raises(StageConflictError, match="Requires stage 1"):
        await client.run_processing_job()


@pytest.mark.asyncio
async def test_stage2_failure_surfaces_exact_server_error():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _json_response(
                202, {"stage": 2, "run_id": "run-2", "status": "RUNNING"}
            )
        return _json_response(
            200,
            _sample_state(
                run_id="run-2",
                stage2={
                    "status": "FAILED",
                    "error": "No Flink TaskManager is registered, so a job cannot run.",
                },
            ),
        )

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    with pytest.raises(StageExecutionError, match="No Flink TaskManager"):
        await client.run_processing_job(poll_interval=0.01, timeout=1.0)


@pytest.mark.asyncio
async def test_stage_polling_timeout_raises_stage_execution_error():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _json_response(
                202, {"stage": 2, "run_id": "run-t", "status": "RUNNING"}
            )
        return _json_response(
            200,
            _sample_state(
                run_id="run-t",
                stage2={
                    "status": "RUNNING",
                    "phase": "Waiting for Flink to report a final state",
                },
            ),
        )

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    with pytest.raises(StageExecutionError, match="timed out"):
        await client.run_processing_job(poll_interval=0.01, timeout=0.05)


@pytest.mark.asyncio
async def test_stage3_enforces_fluss_read_back_verification():
    """Ground Rule 3: Stage 3 fails if Fluss read_back.verified is False or incomplete."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _json_response(
                202, {"stage": 3, "run_id": "run-3", "status": "RUNNING"}
            )
        return _json_response(
            200,
            _sample_state(
                run_id="run-3",
                stage3={
                    "status": "SUCCEEDED",
                    "execution": {
                        "job_id": "job-ingest-1",
                        "job_state": "FINISHED",
                        "read_back": {
                            "verified": False,
                            "checked": 6,
                            "matched": 5,
                            "rows": [],
                        },
                    },
                },
            ),
        )

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    with pytest.raises(StageExecutionError, match="persistence not proven"):
        await client.run_ingestion_job(poll_interval=0.01, timeout=1.0)


@pytest.mark.asyncio
async def test_lookup_uses_real_table_and_query_params():
    """Verifies lookup_supplier/lookup_payment_review hit /api/lookup/{table}?run_id=..."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/state":
            return _json_response(200, _sample_state(run_id="run-live-42"))
        if request.url.path == "/api/lookup/supplier_risk":
            assert request.url.params["run_id"] == "run-live-42"
            assert request.url.params["supplier_id"] == SUPPLIER_ID
            return _json_response(
                200,
                {
                    "table": "payment_risk.supplier_risk",
                    "key": {"run_id": "run-live-42", "supplier_id": SUPPLIER_ID},
                    "ok": True,
                    "found": True,
                    "duration_ms": 1.4,
                    "row": {"context_version": 3, "signal_band": "HIGH"},
                },
            )
        if request.url.path == "/api/lookup/payment_reviews":
            assert request.url.params["run_id"] == "run-live-42"
            assert request.url.params["payment_id"] == PAYMENT_ID
            return _json_response(
                404,
                {
                    "table": "payment_risk.payment_reviews",
                    "key": {"run_id": "run-live-42", "payment_id": PAYMENT_ID},
                    "ok": True,
                    "found": False,
                    "duration_ms": 1.1,
                    "row": None,
                },
            )
        raise AssertionError(f"Unexpected request: {request.url}")

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    sup = await client.lookup_supplier(SUPPLIER_ID)
    assert sup["found"] is True
    assert sup["row"]["signal_band"] == "HIGH"

    rev = await client.lookup_payment_review(PAYMENT_ID)
    assert rev["ok"] is True
    assert rev["found"] is False


@pytest.mark.asyncio
async def test_lookup_503_raises_service_unavailable():
    def handler(request: httpx.Request) -> httpx.Response:
        return _json_response(
            503,
            {
                "table": "payment_risk.supplier_risk",
                "ok": False,
                "found": False,
                "error": "TimeoutException: TabletServer unreachable",
            },
        )

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    with pytest.raises(ServiceUnavailableError, match="TabletServer unreachable"):
        await client.lookup_row(
            "supplier_risk", run_id="run-1", supplier_id=SUPPLIER_ID
        )


def test_lookup_rejects_unknown_table_or_missing_pk():
    import asyncio

    client = StreamhouseClient()
    with pytest.raises(ValueError, match="Unknown Fluss table"):
        asyncio.run(client.lookup_row("not_a_table", run_id="run-1"))


@pytest.mark.asyncio
async def test_send_custom_events_success_and_validation_error():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/events"
        body = json.loads(request.content.decode("utf-8"))
        if body["topic"] == "business-events":
            return _json_response(
                400,
                {
                    "error": "The record was not sent: it does not match the business-event v1 contract.",
                    "record_errors": [{"index": None, "errors": ["missing run_id"]}],
                },
            )
        return _json_response(
            200,
            {
                "records": [
                    {
                        "id": "rec-1234",
                        "topic": "playground-events",
                        "key": body["key"],
                        "status": "ACKNOWLEDGED",
                        "partition": 0,
                        "offset": 7,
                        "send_ms": 4,
                    }
                ]
            },
        )

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    ok = await client.send_custom_events(
        "playground-events", '{"category":"invoice"}', key="inv"
    )
    assert ok["records"][0]["status"] == "ACKNOWLEDGED"
    assert ok["records"][0]["offset"] == 7

    with pytest.raises(StageExecutionError, match="missing run_id"):
        await client.send_custom_events("business-events", '{"invalid":true}')


@pytest.mark.asyncio
async def test_run_sql_polls_until_finished_and_passes_bearer_token():
    polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.method == "POST" and request.url.path == "/api/sql/executions":
            assert request.headers.get("authorization") == "Bearer secret-tok"
            return _json_response(202, {"execution_id": "sql-abcd", "kind": "QUERY"})
        if request.method == "GET" and request.url.path == "/api/state":
            polls += 1
            if polls == 1:
                return _json_response(
                    200,
                    _sample_state(
                        sql_executions=[
                            {"id": "sql-abcd", "status": "RUNNING", "phase": "Running"}
                        ]
                    ),
                )
            return _json_response(
                200,
                _sample_state(
                    sql_executions=[
                        {
                            "id": "sql-abcd",
                            "status": "FINISHED",
                            "kind": "QUERY",
                            "mode": "batch",
                            "job_id": "flink-sql-1",
                            "columns": [{"name": "cnt", "type": "BIGINT"}],
                            "rows": [{"kind": "INSERT", "fields": [4]}],
                            "row_count": 1,
                            "duration_ms": 620,
                        }
                    ]
                ),
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    res = await client.run_sql(
        "SELECT COUNT(*) FROM business_events",
        access_token="secret-tok",
        poll_interval=0.01,
    )
    assert polls == 2
    assert res["status"] == "FINISHED"
    assert res["rows"][0]["fields"] == [4]


@pytest.mark.asyncio
async def test_run_sql_policy_rejection_and_auth_error():
    def handler(request: httpx.Request) -> httpx.Response:
        if not request.headers.get("authorization"):
            return _json_response(401, {"error": "Access token required."})
        return _json_response(400, {"error": "DDL statements are not permitted."})

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    with pytest.raises(StreamhousePipelineError, match="Access token required"):
        await client.run_sql("DROP TABLE business_events")
    with pytest.raises(StageExecutionError, match="DDL statements"):
        await client.run_sql("DROP TABLE business_events", access_token="tok")


@pytest.mark.asyncio
async def test_run_python_polls_and_returns_stdout_and_stderr():
    polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        if request.method == "POST" and request.url.path == "/api/python/executions":
            return _json_response(202, {"execution_id": "py-9999"})
        if request.method == "GET" and request.url.path == "/api/state":
            polls += 1
            if polls == 1:
                return _json_response(
                    200,
                    _sample_state(
                        python_executions=[{"id": "py-9999", "status": "RUNNING"}]
                    ),
                )
            return _json_response(
                200,
                _sample_state(
                    python_executions=[
                        {
                            "id": "py-9999",
                            "status": "FINISHED",
                            "exit_code": 0,
                            "stdout": "supplier_risk: lookup took 1.2 ms\n",
                            "stderr": "",
                            "duration_ms": 145,
                        }
                    ]
                ),
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = StreamhouseClient(transport=httpx.MockTransport(handler))
    res = await client.run_python("print('ok')", poll_interval=0.01)
    assert polls == 2
    assert res["status"] == "FINISHED"
    assert res["exit_code"] == 0
    assert "lookup took 1.2 ms" in res["stdout"]


def test_notebooks_guard_all_execution_calls_with_mo_stop_and_handle_errors():
    """Static compliance guard: no notebook auto-executes stages or user code on page load."""
    guarded_methods = {
        "emit_events",
        "run_processing_job",
        "run_ingestion_job",
        "query_fluss_stage",
        "start_new_run",
        "send_custom_events",
        "run_sql",
        "run_python",
    }
    for nb_path in sorted(NOTEBOOKS_DIR.glob("0*.py")):
        source = nb_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(nb_path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                called_methods = {
                    call.func.attr
                    for call in ast.walk(node)
                    if isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                }
                if called_methods & guarded_methods:
                    assert "stop" in called_methods, (
                        f"{nb_path.name}:{node.lineno} calls {called_methods & guarded_methods} "
                        "without a `mo.stop(...)` button guard (Ground Rule 1 violation)."
                    )
                    has_try_pipeline_err = any(
                        isinstance(h.type, ast.Name)
                        and h.type.id == StreamhousePipelineError.__name__
                        for stmt in ast.walk(node)
                        if isinstance(stmt, ast.Try)
                        for h in stmt.handlers
                    )
                    assert has_try_pipeline_err, (
                        f"{nb_path.name}:{node.lineno} must catch StreamhousePipelineError "
                        "to render truthful stage/execution error messages."
                    )
