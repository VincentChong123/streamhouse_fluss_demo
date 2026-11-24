import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    from shared.client import StreamhouseClient, StreamhousePipelineError
    from shared.render import render_custom_events_result, render_error_callout
    from shared.scenario import NORTHSTAR_EVENTS, SCENARIO_TITLE, SUPPLIER_ID

    mo.md(
        f"""
        # Phase 1: Apache Kafka Events

        Stage 1 emits the four canonical *{SCENARIO_TITLE}* business events to Kafka topic
        `business-events` through the live `demo-app` producer (`acks=all`, idempotent).

        | Streaming Concept | Role in Stage 1 |
        | :--- | :--- |
        | **Business Time (`occurred_at`)** | Event time (10:00–10:03 UTC) evaluated by Flink's payment rule, distinct from processing time. |
        | **Partition Key (`run_id:supplier_id`)** | Routes every `{SUPPLIER_ID}` event for this run to the same partition so causal ordering is preserved. |
        | **Broker Acknowledgment (`acks=all`)** | Records are marked `ACKNOWLEDGED` only after physical commit by Kafka. |
        | **Sequential Offsets (`p0 @ offset`)** | Stage 2 uses the captured bounded offset interval `[start, end)` for deterministic processing. |
        """
    )
    return (
        NORTHSTAR_EVENTS,
        StreamhouseClient,
        StreamhousePipelineError,
        mo,
        render_custom_events_result,
        render_error_callout,
    )


@app.cell
def _(NORTHSTAR_EVENTS, mo):
    mo.accordion(
        {
            "Canonical Scenario Reference (4 events to be emitted by demo-app)": mo.ui.table(
                NORTHSTAR_EVENTS
            )
        }
    )
    return


@app.cell
def _(mo):
    app_url = mo.ui.text(value="http://localhost:8090", label="demo-app URL")
    btn_health = mo.ui.run_button(label="Check Kafka Health")
    btn_new_run = mo.ui.run_button(label="Start New Demo Run")
    btn_emit = mo.ui.run_button(label="Emit Scenario Events to Kafka")
    mo.hstack(
        [app_url, btn_health, btn_new_run, btn_emit], align="end", gap=1, wrap=True
    )
    return app_url, btn_emit, btn_health, btn_new_run


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_health,
    mo,
    render_error_callout,
):
    mo.stop(not btn_health.value)
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _state = await _client.get_state()
        _comps = {
            c["id"]: c for c in (_state.get("health") or {}).get("components", [])
        }
        _kafka = _comps.get("kafka") or {}
        _run = _state.get("run") or {}
        _s1 = _run.get("stage1") or {}
        _out = mo.callout(
            mo.md(
                f"**Kafka status:** `{_kafka.get('status', 'UNKNOWN')}` — {_kafka.get('detail', '')}\n\n"
                f"**Active run:** `{_run.get('run_id', 'none')}` · "
                f"**Stage 1 status:** `{_s1.get('status', 'UNKNOWN')}` "
                f"(`can_run={_s1.get('can_run')}`"
                + (f", {_s1['blocked_reason']}" if _s1.get("blocked_reason") else "")
                + ")"
            ),
            kind="info" if _kafka.get("status") == "UP" else "warn",
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Kafka health check failed", _exc)
    _out
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_new_run,
    mo,
    render_error_callout,
):
    mo.stop(not btn_new_run.value)
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _run = await _client.start_new_run()
        _out = mo.callout(
            mo.md(
                f"✅ Created fresh run **`{_run.get('run_id')}`** — ready to emit Stage 1 events."
            ),
            kind="success",
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Failed to create new run", _exc)
    _out
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_emit,
    mo,
    render_error_callout,
):
    mo.stop(
        not btn_emit.value,
        mo.md("*Click **Emit Scenario Events to Kafka** above to run Stage 1.*"),
    )
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _result = await _client.emit_events()
        _events = _result["events"]
        _table_rows = [
            {
                "business_time": ev.get("occurred_at"),
                "event_type": ev.get("event_type"),
                "event_id": ev.get("event_id"),
                "partition_key": ev.get("key"),
                "status": ev.get("status"),
                "partition": ev.get("partition"),
                "offset": ev.get("offset"),
                "send_ms": ev.get("send_ms"),
                "acked_at": ev.get("acked_at"),
            }
            for ev in _events
        ]
        _offsets = [ev["offset"] for ev in _events]
        _out = mo.vstack(
            [
                mo.callout(
                    mo.md(
                        f"### ✅ Stage 1 SUCCEEDED\n"
                        f"- **Run ID:** `{_result['run_id']}`\n"
                        f"- **Topic:** `{_result['topic']}` (offsets `{min(_offsets)}…{max(_offsets)}`)\n"
                        f"- **Acknowledged Records:** `{len(_events)}` (`acks=all`)\n"
                        f"- **Total Duration:** `{_result.get('duration_ms')}` ms"
                    ),
                    kind="success",
                ),
                mo.ui.table(_table_rows, label="Acknowledged Kafka Records"),
                mo.accordion(
                    {
                        "Stage Step Timings": mo.ui.table(_result.get("steps", [])),
                        "Raw Event Payloads Sent to Kafka": mo.json(
                            [ev.get("record") for ev in _events]
                        ),
                    }
                ),
            ]
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Stage 1 failed", _exc)
    _out
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## User Input Cell: Send Custom Event(s) to Kafka (`POST /api/events`)

        Enter a custom JSON object (or JSON array / JSONL when **Batch mode** is checked).
        `playground-events` accepts any JSON object; `business-events` enforces the
        `business-event/v1` contract and rejects invalid records before sending.
        """
    )
    custom_topic = mo.ui.dropdown(
        options=["playground-events", "business-events"],
        value="playground-events",
        label="Target Topic",
    )
    custom_key = mo.ui.text(value="invoice", label="Message Key (optional)")
    custom_batch = mo.ui.checkbox(value=False, label="Batch mode (JSON array or JSONL)")
    btn_load_templates = mo.ui.run_button(label="Fetch Server Event Templates")
    custom_payload_editor = mo.ui.code_editor(
        value=(
            "{\n"
            '  "event_id": "pg-custom-001",\n'
            '  "category": "invoice",\n'
            '  "amount": 1250.50,\n'
            '  "currency": "USD"\n'
            "}"
        ),
        language="json",
        min_height=180,
        label="Event JSON Payload",
    )
    btn_send_custom = mo.ui.run_button(label="Send Custom Event to Kafka")
    mo.vstack(
        [
            mo.hstack(
                [custom_topic, custom_key, custom_batch, btn_load_templates],
                align="end",
                gap=1,
                wrap=True,
            ),
            custom_payload_editor,
            btn_send_custom,
        ]
    )
    return (
        btn_load_templates,
        btn_send_custom,
        custom_batch,
        custom_key,
        custom_payload_editor,
        custom_topic,
    )


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_load_templates,
    mo,
    render_error_callout,
):
    mo.stop(not btn_load_templates.value)
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _templates = await _client.get_event_templates()
        _out = mo.accordion(
            {
                f"Live Event Templates from demo-app ({len(_templates)} available)": mo.json(
                    _templates
                )
            }
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Failed to load templates", _exc)
    _out
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_send_custom,
    custom_batch,
    custom_key,
    custom_payload_editor,
    custom_topic,
    mo,
    render_custom_events_result,
    render_error_callout,
):
    mo.stop(
        not btn_send_custom.value,
        mo.md(
            "### Execution Output\n*Click **Send Custom Event to Kafka** above to see broker acknowledgments.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _resp = await _client.send_custom_events(
            topic=custom_topic.value,
            payload=custom_payload_editor.value,
            key=custom_key.value,
            batch=custom_batch.value,
        )
        _view = render_custom_events_result(mo, _resp)
    except StreamhousePipelineError as _exc:
        _view = render_error_callout(mo, "Custom Kafka send failed", _exc)
    mo.vstack([mo.md("### Execution Output"), _view])
    return


if __name__ == "__main__":
    app.run()
