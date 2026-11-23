import marimo

__generated_with = "0.25.2"
app = marimo.App(width="wide")


@app.cell
def __():
    import marimo as mo
    from shared.client import StreamhouseClient
    from shared.scenario import NORTHSTAR_EVENTS

    mo.md(
        """
        # 🛡️ End-to-End Streamhouse Simulation Playbook
        Interactive simulation combining **Kafka**, **Flink**, and **Fluss**.
        """
    )
    return mo, StreamhouseClient, NORTHSTAR_EVENTS


@app.cell
def __(mo):
    step = mo.ui.radio(
        options=[
            "1. Emit Kafka Events",
            "2. Run Flink Correlation",
            "3. Ingest into Fluss",
            "4. Query Risk Review",
        ],
        value="1. Emit Kafka Events",
        label="Select Simulation Step:",
    )
    step  # noqa: B018
    return (step,)


@app.cell
async def __(StreamhouseClient, mo, step, NORTHSTAR_EVENTS):
    _client = StreamhouseClient()
    if step.value == "1. Emit Kafka Events":
        _acks = await _client.emit_events(NORTHSTAR_EVENTS)
        _output = mo.ui.table(_acks)
    elif step.value == "2. Run Flink Correlation":
        _res = await _client.run_processing_job()
        _output = mo.json(_res)
    elif step.value == "3. Ingest into Fluss":
        _res = await _client.run_ingestion_job()
        _output = mo.json(_res)
    else:
        _res = await _client.lookup_supplier()
        _output = mo.json(_res)
    _output  # noqa: B018


if __name__ == "__main__":
    app.run()
