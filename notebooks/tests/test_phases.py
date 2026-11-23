import pytest
from shared.client import StreamhouseClient
from shared.scenario import NORTHSTAR_EVENTS, SUPPLIER_ID


@pytest.fixture
def client():
    return StreamhouseClient()


@pytest.mark.asyncio
async def test_phase1_kafka(client):
    acks = await client.emit_events(NORTHSTAR_EVENTS)
    assert len(acks) == 4
    for ack in acks:
        assert ack["status"] == "ACKNOWLEDGED"
        assert ack["partition"] == 0


@pytest.mark.asyncio
async def test_phase2_flink(client):
    result = await client.run_processing_job(start_offset=0, end_offset=4)
    assert result["state"] == "FINISHED"


@pytest.mark.asyncio
async def test_phase3_fluss(client):
    ingest = await client.run_ingestion_job()
    assert ingest["state"] == "FINISHED"
    row = await client.lookup_supplier(SUPPLIER_ID)
    assert row["supplier_id"] == SUPPLIER_ID
    assert row["context_version"] == 3
    assert row["signal_band"] == "HIGH"
    assert row["outcome"] == "MANUAL_REVIEW"


@pytest.mark.asyncio
async def test_simulation(client):
    # E2E test running all steps sequentially
    acks = await client.emit_events(NORTHSTAR_EVENTS)
    assert len(acks) == 4
    proc = await client.run_processing_job()
    assert proc["state"] == "FINISHED"
    ingest = await client.run_ingestion_job()
    assert ingest["state"] == "FINISHED"
    query = await client.lookup_supplier(SUPPLIER_ID)
    assert query["outcome"] == "MANUAL_REVIEW"
