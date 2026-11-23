import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    from shared.client import StreamhouseClient
    from shared.scenario import NORTHSTAR_EVENTS

    mo.md(
        """
        # 📨 Phase 1: Apache Kafka Events
        This notebook demonstrates streaming ingestion of payment events into Kafka.
        - **Topic:** `business-events`
        - **Partitioning:** Keyed by `supplier_id` to guarantee strictly ordered delivery.
        """
    )
    return mo, StreamhouseClient, NORTHSTAR_EVENTS


@app.cell
def __(NORTHSTAR_EVENTS, mo):
    btn_emit = mo.ui.run_button(label="Emit Scenario Events to Kafka")
    btn_emit  # noqa: B018
    return (btn_emit,)


@app.cell
async def __(btn_emit, NORTHSTAR_EVENTS, StreamhouseClient, mo):
    mo.stop(
        not btn_emit.value, mo.md("*Click the button above to emit events to Kafka.*")
    )
    _client = StreamhouseClient()
    _acks = await _client.emit_events(NORTHSTAR_EVENTS)
    mo.ui.table(_acks)


if __name__ == "__main__":
    app.run()
