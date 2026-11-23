"""Genuine end-to-end integration tests against the live Streamhouse Docker Compose stack.

Per CONTRIBUTING.md and README.md ("No mocked integration tests"), every test
in this module requires real Kafka, Flink, Fluss, and demo-app services running
at ``STREAMHOUSE_APP_URL`` (default ``http://localhost:8090``).

Run offline unit tests instead with:
    uv run --extra test pytest -v -m "not integration"

Run these live tests after ``docker compose up --build -d`` with:
    uv run --extra test pytest -v -m integration
"""

from __future__ import annotations

import pytest

from shared.client import StageConflictError, StreamhouseClient
from shared.scenario import PAYMENT_ID, SUPPLIER_ID

pytestmark = pytest.mark.integration


@pytest.fixture
def client() -> StreamhouseClient:
    return StreamhouseClient()


@pytest.mark.asyncio
async def test_phase1_kafka(client: StreamhouseClient):
    """Fresh run -> Stage 2 blocked before Stage 1 -> Stage 1 emits 4 real Kafka acks."""
    run = await client.start_new_run()
    run_id = run["run_id"]

    with pytest.raises(StageConflictError):
        await client.run_processing_job()

    stage1 = await client.emit_events()
    assert stage1["run_id"] == run_id
    assert stage1["status"] == "SUCCEEDED"
    events = stage1["events"]
    assert len(events) == 4
    for ev in events:
        assert ev["status"] == "ACKNOWLEDGED"
        assert ev["partition"] == 0
        assert isinstance(ev["offset"], int)

    offsets = [ev["offset"] for ev in events]
    assert offsets == list(range(offsets[0], offsets[0] + 4))

    # Ground Rule 2/3: Fluss must have no rows for this run before Stage 3.
    pre_lookup = await client.lookup_supplier(SUPPLIER_ID, run_id=run_id)
    assert pre_lookup["ok"] is True
    assert pre_lookup["found"] is False


@pytest.mark.asyncio
async def test_phase2_flink(client: StreamhouseClient):
    """Runs Stage 1 -> Stage 2 bounded Flink job and proves Fluss is still empty."""
    run = await client.start_new_run()
    run_id = run["run_id"]
    await client.emit_events()

    stage2 = await client.run_processing_job()
    assert stage2["run_id"] == run_id
    assert stage2["status"] == "SUCCEEDED"
    execution = stage2["execution"]
    assert execution["job_state"] == "FINISHED"
    assert execution["job_id"]
    assert execution["input_records_this_run"] == 4

    supplier_versions = [
        r["row"]["context_version"]
        for r in execution["results"]
        if r.get("target_table") == "supplier_risk"
    ]
    assert supplier_versions == [1, 2, 3]

    reviews = [
        r["row"]
        for r in execution["results"]
        if r.get("target_table") == "payment_reviews"
    ]
    assert len(reviews) == 1
    assert reviews[0]["outcome"] == "MANUAL_REVIEW"

    # Between Stage 2 and Stage 3, computed results exist only in Kafka.
    pre_lookup = await client.lookup_supplier(SUPPLIER_ID, run_id=run_id)
    assert pre_lookup["ok"] is True
    assert pre_lookup["found"] is False


@pytest.mark.asyncio
async def test_phase3_and_phase4_fluss(client: StreamhouseClient):
    """Full 4-stage pipeline: Kafka -> Flink -> Fluss Ingestion+ReadBack -> Native Lookups."""
    run = await client.start_new_run()
    run_id = run["run_id"]

    await client.emit_events()
    await client.run_processing_job()

    stage3 = await client.run_ingestion_job()
    assert stage3["run_id"] == run_id
    assert stage3["status"] == "SUCCEEDED"
    ingest_exec = stage3["execution"]
    assert ingest_exec["job_state"] == "FINISHED"
    read_back = ingest_exec["read_back"]
    assert read_back["verified"] is True
    assert read_back["checked"] == 6
    assert read_back["matched"] == 6

    stage4 = await client.query_fluss_stage()
    assert stage4["run_id"] == run_id
    assert stage4["status"] == "SUCCEEDED"
    supplier_row = stage4["result"]["supplier"]["row"]
    review_row = stage4["result"]["review"]["row"]

    assert supplier_row["supplier_id"] == SUPPLIER_ID
    assert supplier_row["context_version"] == 3
    assert supplier_row["signal_band"] == "HIGH"
    assert supplier_row["unfamiliar_device_evidence"] is True

    assert review_row["payment_id"] == PAYMENT_ID
    assert review_row["outcome"] == "MANUAL_REVIEW"
    assert review_row["check_recent_change"] == "TRUE"
    assert review_row["check_unfamiliar_device"] == "TRUE"
    assert review_row["check_large_payment"] == "TRUE"

    # Also verify direct /api/lookup/{table} endpoint returns matching rows.
    direct_sup = await client.lookup_supplier(SUPPLIER_ID, run_id=run_id)
    assert direct_sup["ok"] is True and direct_sup["found"] is True
    assert direct_sup["row"]["signal_band"] == "HIGH"

    direct_rev = await client.lookup_payment_review(PAYMENT_ID, run_id=run_id)
    assert direct_rev["ok"] is True and direct_rev["found"] is True
    assert direct_rev["row"]["outcome"] == "MANUAL_REVIEW"


@pytest.mark.asyncio
async def test_interactive_workspaces_kafka_sql_python(client: StreamhouseClient):
    """Verifies real user-input executions across Kafka, Flink SQL Gateway, and python-runner."""
    sent = await client.send_custom_events(
        topic="playground-events",
        payload='{"event_id":"pg-it-1","category":"invoice","amount":420.0,"currency":"USD"}',
        key="invoice",
    )
    assert sent["records"][0]["status"] == "ACKNOWLEDGED"

    sql_res = await client.run_sql("SHOW TABLES", mode="batch", workspace="sql")
    assert sql_res["status"] == "FINISHED"
    assert sql_res["row_count"] >= 3

    py_res = await client.run_python('print("streamhouse-marimo-ok")')
    assert py_res["status"] == "FINISHED"
    assert py_res["exit_code"] == 0
    assert "streamhouse-marimo-ok" in py_res["stdout"]
