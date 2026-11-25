import marimo

__generated_with = "0.25.2"
app = marimo.App(width="wide")


@app.cell
def _():
    import marimo as mo

    from shared.client import StreamhouseClient, StreamhousePipelineError
    from shared.render import (
        render_custom_events_result,
        render_error_callout,
        render_python_result,
        render_sql_result,
        utc_timestamp,
    )
    from shared.scenario import (
        PAYMENT_ID,
        RULE_SUMMARY,
        SCENARIO_SUMMARY,
        SCENARIO_TITLE,
        SUPPLIER_ID,
    )

    mo.md(
        f"""
        # End-to-End Streamhouse Simulation Playbook

        Interactive, button-driven control plane for the **Payment Risk Streamhouse Demo**
        (**Apache Kafka → Apache Flink → Apache Fluss**), driving the live `demo-app` REST API
        alongside `web/v2.html`.

        > **Scenario:** *{SCENARIO_TITLE}* — {SCENARIO_SUMMARY}
        >
        > **Rule:** `{RULE_SUMMARY}`
        >
        > **Ground rules enforced:** Nothing runs on page load. Every stage and interactive query
        > is triggered by its own button, polled until completion, and displays only real results
        > or errors from Kafka, Flink, Fluss, and `python-runner`.
        """
    )
    return (
        PAYMENT_ID,
        SUPPLIER_ID,
        StreamhouseClient,
        StreamhousePipelineError,
        mo,
        render_custom_events_result,
        render_error_callout,
        render_python_result,
        render_sql_result,
        utc_timestamp,
    )


@app.cell
def _(mo):
    app_url_input = mo.ui.text(
        value="http://localhost:8090",
        label="demo-app URL",
        full_width=False,
    )
    btn_health = mo.ui.run_button(label="Inspect Cluster Health & Run State")
    btn_new_run = mo.ui.run_button(label="Start New Demo Run")
    mo.hstack([app_url_input, btn_health, btn_new_run], align="end", gap=1)
    return app_url_input, btn_health, btn_new_run


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url_input,
    btn_health,
    mo,
    render_error_callout,
    utc_timestamp,
):
    mo.stop(
        not btn_health.value,
        mo.md(
            "*Click **Inspect Cluster Health & Run State** to query `GET /api/state` (read-only).*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url_input.value)
    try:
        _state = await _client.get_state()
        _health = _state.get("health") or {}
        _run = _state.get("run") or {}
        _comp_rows = [
            {
                "component": c.get("label"),
                "required": c.get("required"),
                "status": c.get("status"),
                "detail": c.get("detail"),
                "hint": c.get("hint") or "",
            }
            for c in _health.get("components", [])
        ]
        _stage_rows = []
        for _n, _label in [
            (1, "1 · Events (Kafka)"),
            (2, "2 · Processing (Flink)"),
            (3, "3 · Ingestion (Fluss)"),
            (4, "4 · Query (Fluss)"),
        ]:
            _st = _run.get(f"stage{_n}") or {}
            _stage_err = _st.get("error")
            if _stage_err:
                _err_ts = (
                    _st.get("finished_at")
                    or _st.get("started_at")
                    or utc_timestamp()
                )
                _blocked_or_error = f"[{_err_ts}] {_stage_err}"
            else:
                _blocked_or_error = _st.get("blocked_reason") or ""
            _stage_rows.append(
                {
                    "stage": _label,
                    "status": _st.get("status"),
                    "can_run": _st.get("can_run"),
                    "duration_ms": _st.get("duration_ms"),
                    "blocked_or_error": _blocked_or_error,
                }
            )
        _view = mo.vstack(
            [
                mo.md(
                    f"### Cluster Health: `{_health.get('overall', 'UNKNOWN')}` · "
                    f"Active Run: `{_run.get('run_id', 'none')}`"
                ),
                mo.ui.table(_comp_rows, label="Infrastructure Components"),
                mo.ui.table(_stage_rows, label="Stage Gates & Status"),
            ]
        )
    except StreamhousePipelineError as _exc:
        _view = render_error_callout(mo, "Health check failed", _exc)
    _view
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url_input,
    btn_new_run,
    mo,
    render_error_callout,
):
    mo.stop(not btn_new_run.value)
    _client = StreamhouseClient(app_url=app_url_input.value)
    try:
        _res = await _client.start_new_run()
        _out = mo.callout(
            mo.md(
                f"✅ Started fresh demo run **`{_res.get('run_id')}`**. "
                "Topics and tables are kept; earlier runs' data stays isolated by `run_id`."
            ),
            kind="success",
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Could not start new run", _exc)
    _out
    return


@app.cell
def _(mo):
    mo.md("## Stage Controls (Execute Sequentially: 1 → 2 → 3 → 4)")
    btn_stage1 = mo.ui.run_button(label="Stage 1: Emit Kafka Events")
    btn_stage2 = mo.ui.run_button(label="Stage 2: Run Flink Processing")
    btn_stage3 = mo.ui.run_button(label="Stage 3: Ingest into Fluss & Verify Read-Back")
    btn_stage4 = mo.ui.run_button(label="Stage 4: Query Fluss (Native Lookups)")
    mo.hstack([btn_stage1, btn_stage2, btn_stage3, btn_stage4], gap=1, wrap=True)
    return btn_stage1, btn_stage2, btn_stage3, btn_stage4


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url_input,
    btn_stage1,
    mo,
    render_error_callout,
):
    mo.stop(
        not btn_stage1.value,
        mo.md(
            "**Stage 1 (Kafka):** *Idle — click **Stage 1: Emit Kafka Events** to produce to `business-events`.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url_input.value)
    try:
        _s1 = await _client.emit_events()
        _rows = [
            {
                "occurred_at": ev.get("occurred_at"),
                "event_type": ev.get("event_type"),
                "event_id": ev.get("event_id"),
                "key": ev.get("key"),
                "status": ev.get("status"),
                "partition": ev.get("partition"),
                "offset": ev.get("offset"),
                "send_ms": ev.get("send_ms"),
            }
            for ev in _s1["events"]
        ]
        _out = mo.vstack(
            [
                mo.callout(
                    mo.md(
                        f"✅ **Stage 1 SUCCEEDED** · Run `{_s1['run_id']}` · "
                        f"Topic `{_s1['topic']}` · {len(_rows)} records acknowledged (`acks=all`) "
                        f"in `{_s1.get('duration_ms')}` ms"
                    ),
                    kind="success",
                ),
                mo.ui.table(_rows, label="Kafka Broker Acknowledgments"),
            ]
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Stage 1 failed", _exc)
    _out
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url_input,
    btn_stage2,
    mo,
    render_error_callout,
):
    mo.stop(
        not btn_stage2.value,
        mo.md(
            "**Stage 2 (Flink Processing):** *Idle — requires Stage 1 to succeed first.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url_input.value)
    try:
        _s2 = await _client.run_processing_job()
        _exec = _s2["execution"]
        _results = _exec.get("results", [])
        _supplier_versions = [
            {
                "offset": r.get("offset"),
                "context_version": (r.get("row") or {}).get("context_version"),
                "signal_band": (r.get("row") or {}).get("signal_band"),
                "current_beneficiary": (r.get("row") or {}).get(
                    "current_beneficiary_masked"
                ),
                "unfamiliar_device": (r.get("row") or {}).get(
                    "unfamiliar_device_evidence"
                ),
                "last_event": (r.get("row") or {}).get("last_source_event_type"),
            }
            for r in _results
            if r.get("target_table") == "supplier_risk"
        ]
        _out = mo.vstack(
            [
                mo.callout(
                    mo.md(
                        f"✅ **Stage 2 SUCCEEDED** · Run `{_s2['run_id']}` · "
                        f"Flink Job [`{_exec.get('job_id')}`]({_exec.get('job_url')}) (`{_exec.get('job_state')}`) · "
                        f"Input `{_exec.get('input_topic')}` `[{_exec.get('input_start')}, {_exec.get('input_end')})` "
                        f"(`{_exec.get('input_records_this_run')}` records) → "
                        f"Output `{_exec.get('output_topic')}` `[{_exec.get('output_start')}, {_exec.get('output_end')})` "
                        f"(`{len(_results)}` computed results). *Fluss tables are still untouched.*"
                    ),
                    kind="success",
                ),
                mo.ui.table(
                    _supplier_versions,
                    label="Supplier Context Evolution (from Kafka computed-results)",
                ),
            ]
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Stage 2 failed", _exc)
    _out
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url_input,
    btn_stage3,
    mo,
    render_error_callout,
):
    mo.stop(
        not btn_stage3.value,
        mo.md(
            "**Stage 3 (Fluss Ingestion):** *Idle — requires Stage 2 to succeed first.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url_input.value)
    try:
        _s3 = await _client.run_ingestion_job()
        _exec = _s3["execution"]
        _rb = _exec["read_back"]
        _rb_rows = [
            {
                "table": row.get("table"),
                "key": str(row.get("key")),
                "matches": row.get("matches"),
                "duration_ms": round(
                    (row.get("observation") or {}).get("duration_ms", 0.0), 2
                ),
                "computed_by": row.get("computed_by"),
            }
            for row in _rb.get("rows", [])
        ]
        _out = mo.vstack(
            [
                mo.callout(
                    mo.md(
                        f"✅ **Stage 3 SUCCEEDED** · Run `{_s3['run_id']}` · "
                        f"Flink Ingestion Job [`{_exec.get('job_id')}`]({_exec.get('job_url')}) (`{_exec.get('job_state')}`) · "
                        f"**Native Fluss Read-Back:** `{_rb['matched']}/{_rb['checked']}` rows confirmed "
                        f"(`verified={_rb['verified']}`)"
                    ),
                    kind="success",
                ),
                mo.ui.table(_rb_rows, label="Native Fluss Read-Back Verification"),
            ]
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Stage 3 failed", _exc)
    _out
    return


@app.cell
async def _(
    PAYMENT_ID,
    SUPPLIER_ID,
    StreamhouseClient,
    StreamhousePipelineError,
    app_url_input,
    btn_stage4,
    mo,
    render_error_callout,
):
    mo.stop(
        not btn_stage4.value,
        mo.md(
            f"**Stage 4 (Fluss Native Lookups):** *Idle — requires Stage 3 to confirm `{SUPPLIER_ID}` and `{PAYMENT_ID}` first.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url_input.value)
    try:
        _s4 = await _client.query_fluss_stage()
        _res = _s4["result"]
        _sup = _res["supplier"]
        _rev = _res["review"]
        _sup_row = _sup.get("row") or {}
        _rev_row = _rev.get("row") or {}
        _out = mo.vstack(
            [
                mo.callout(
                    mo.md(
                        f"✅ **Stage 4 SUCCEEDED** · Run `{_s4['run_id']}` · "
                        f"`supplier_risk`: **v{_sup_row.get('context_version')} / {_sup_row.get('signal_band')}** "
                        f"(`{_sup.get('duration_ms', 0):.1f}` ms) · "
                        f"`payment_reviews`: **{_rev_row.get('outcome')}** "
                        f"(`{_rev.get('duration_ms', 0):.1f}` ms)\n\n"
                        f"**Explanation:** {_rev_row.get('explanation', '')}"
                    ),
                    kind="success",
                ),
                mo.json(_res),
            ]
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Stage 4 failed", _exc)
    _out
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## Interactive Execution Lab (User Input & Output)

        Choose a target runtime (**Kafka JSON Event**, **Flink SQL**, or **Python `pyfluss`**),
        edit the input below, and click **Execute User Input** to run against the live stack and
        view the execution result in the output cell.
        """
    )
    lab_target = mo.ui.dropdown(
        options=["Flink SQL", "Kafka Event (JSON)", "Python (pyfluss)"],
        value="Flink SQL",
        label="Execution Target",
    )
    lab_kafka_topic = mo.ui.dropdown(
        options=["playground-events", "business-events"],
        value="playground-events",
        label="Kafka Topic (for JSON target)",
    )
    lab_kafka_key = mo.ui.text(
        value="invoice",
        label="Kafka Key (optional)",
    )
    lab_token = mo.ui.text(
        value="",
        kind="password",
        label="Access Token (optional)",
    )
    mo.hstack(
        [lab_target, lab_kafka_topic, lab_kafka_key, lab_token],
        align="end",
        gap=1,
        wrap=True,
    )
    return lab_kafka_key, lab_kafka_topic, lab_target, lab_token


@app.cell
def _(lab_target, mo):
    _defaults = {
        "Flink SQL": (
            "sql",
            "SELECT event_type, COUNT(*) AS events\n"
            "FROM business_events\n"
            "GROUP BY event_type",
        ),
        "Kafka Event (JSON)": (
            "json",
            "{\n"
            '  "event_id": "pg-marimo-001",\n'
            '  "category": "invoice",\n'
            '  "amount": 1250.50,\n'
            '  "currency": "USD"\n'
            "}",
        ),
        "Python (pyfluss)": (
            "python",
            "import asyncio, os\n"
            "import fluss\n\n"
            "async def main():\n"
            '    cfg = fluss.Config({"bootstrap.servers": os.environ["FLUSS_BOOTSTRAP_SERVERS"]})\n'
            "    conn = await fluss.FlussConnection.create(cfg)\n"
            "    try:\n"
            "        admin = await conn.get_admin()\n"
            '        tables = await admin.list_tables("payment_risk")\n'
            '        print("Fluss tables in payment_risk:", tables)\n'
            "    finally:\n"
            "        await conn.close()\n\n"
            "asyncio.run(main())\n",
        ),
    }
    _lang, _initial = _defaults[lab_target.value]
    user_input_editor = mo.ui.code_editor(
        value=_initial,
        language=_lang,
        min_height=180,
        label=f"User Input ({lab_target.value})",
    )
    btn_run_user_input = mo.ui.run_button(label=f"Execute {lab_target.value}")
    mo.vstack([user_input_editor, btn_run_user_input])
    return btn_run_user_input, user_input_editor


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url_input,
    btn_run_user_input,
    lab_kafka_key,
    lab_kafka_topic,
    lab_target,
    lab_token,
    mo,
    render_custom_events_result,
    render_error_callout,
    render_python_result,
    render_sql_result,
    user_input_editor,
):
    mo.stop(
        not btn_run_user_input.value,
        mo.md(
            "### Execution Output\n"
            "*Edit the user input above and click the **Execute** button to display live results here.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url_input.value)
    _token = lab_token.value.strip() or None
    try:
        if lab_target.value == "Kafka Event (JSON)":
            _resp = await _client.send_custom_events(
                topic=lab_kafka_topic.value,
                payload=user_input_editor.value,
                key=lab_kafka_key.value,
                batch=False,
            )
            _result_view = render_custom_events_result(mo, _resp)
        elif lab_target.value == "Flink SQL":
            _exec = await _client.run_sql(
                statement=user_input_editor.value,
                mode="batch",
                workspace="sql",
                access_token=_token,
            )
            _result_view = render_sql_result(mo, _exec)
        else:
            _exec = await _client.run_python(
                code=user_input_editor.value,
                access_token=_token,
            )
            _result_view = render_python_result(mo, _exec)
    except StreamhousePipelineError as _exc:
        _result_view = render_error_callout(
            mo, f"{lab_target.value} execution failed", _exc
        )
    mo.vstack([mo.md("### Execution Output"), _result_view])
    return


if __name__ == "__main__":
    app.run()
