import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    from shared.client import StreamhouseClient, StreamhousePipelineError
    from shared.render import render_error_callout, render_sql_result
    from shared.scenario import RULE_SUMMARY, SUPPLIER_ID

    mo.md(
        f"""
        # Phase 2: Apache Flink Bounded Processing

        Stage 2 submits a **bounded** Flink processing job over this run's captured Kafka offset
        range `[input_start, input_end)` on `business-events`.

        - **Stateful Correlation:** Evolves `{SUPPLIER_ID}` context across `v1 (NORMAL)` → `v2 (ELEVATED)` → `v3 (HIGH)`.
        - **Deterministic Rule:** `{RULE_SUMMARY}`
        - **Stage Separation (Ground Rule 2):** Computed results are written **only** to Kafka topic
          `computed-results`. Fluss tables remain empty for this run until Stage 3 runs.
        """
    )
    return (
        SUPPLIER_ID,
        StreamhouseClient,
        StreamhousePipelineError,
        mo,
        render_error_callout,
        render_sql_result,
    )


@app.cell
def _(mo):
    app_url = mo.ui.text(value="http://localhost:8090", label="demo-app URL")
    btn_proc = mo.ui.run_button(label="Run Flink Processing Job")
    btn_probe_fluss = mo.ui.run_button(label="Check Fluss Pre-Ingestion (Read-Only)")
    mo.hstack([app_url, btn_proc, btn_probe_fluss], align="end", gap=1, wrap=True)
    return app_url, btn_probe_fluss, btn_proc


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_proc,
    mo,
    render_error_callout,
):
    mo.stop(
        not btn_proc.value,
        mo.md(
            "*Click **Run Flink Processing Job** above to submit the bounded Flink job.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _stage2 = await _client.run_processing_job()
        _exec = _stage2["execution"]
        _results = _exec.get("results", [])

        _supplier_rows = [
            {
                "kafka_offset": r.get("offset"),
                "context_version": (r.get("row") or {}).get("context_version"),
                "signal_band": (r.get("row") or {}).get("signal_band"),
                "beneficiary": (r.get("row") or {}).get("current_beneficiary_masked"),
                "unfamiliar_device": (r.get("row") or {}).get(
                    "unfamiliar_device_evidence"
                ),
                "source_event": (r.get("row") or {}).get("last_source_event_type"),
                "signal_reason": (r.get("row") or {}).get("signal_reason"),
            }
            for r in _results
            if r.get("target_table") == "supplier_risk"
        ]
        _review_rows = [
            {
                "kafka_offset": r.get("offset"),
                "payment_id": (r.get("row") or {}).get("payment_id"),
                "outcome": (r.get("row") or {}).get("outcome"),
                "context_version_used": (r.get("row") or {}).get(
                    "context_version_used"
                ),
                "recent_change": (r.get("row") or {}).get("check_recent_change"),
                "unfamiliar_device": (r.get("row") or {}).get(
                    "check_unfamiliar_device"
                ),
                "large_payment": (r.get("row") or {}).get("check_large_payment"),
                "explanation": (r.get("row") or {}).get("explanation"),
            }
            for r in _results
            if r.get("target_table") == "payment_reviews"
        ]

        _job_id = _exec.get("job_id")
        _job_url = _exec.get("job_url")
        _job_link = f"[`{_job_id}`]({_job_url})" if _job_url else f"`{_job_id}`"

        _out = mo.vstack(
            [
                mo.callout(
                    mo.md(
                        f"### ✅ Stage 2 Flink Processing SUCCEEDED\n"
                        f"- **Run ID:** `{_stage2['run_id']}`\n"
                        f"- **Flink Job ID:** {_job_link} (`{_exec.get('job_state')}`)\n"
                        f"- **Job Duration:** `{_exec.get('job_duration_ms')}` ms "
                        f"(stage wall time `{_stage2.get('duration_ms')}` ms)\n"
                        f"- **Input Range:** `{_exec.get('input_topic')}` "
                        f"`[{_exec.get('input_start')}, {_exec.get('input_end')})` "
                        f"(`{_exec.get('input_records_this_run')}` record(s) of this run)\n"
                        f"- **Output Range:** `{_exec.get('output_topic')}` "
                        f"`[{_exec.get('output_start')}, {_exec.get('output_end')})` "
                        f"(`{len(_results)}` computed result(s) read back from Kafka)"
                    ),
                    kind="success",
                ),
                mo.ui.table(
                    _supplier_rows, label="Supplier Context Evolution (v1 → v2 → v3)"
                ),
                mo.ui.table(
                    _review_rows,
                    label="Computed Payment Review (in Kafka computed-results)",
                ),
                mo.accordion(
                    {
                        "Flink Accumulators": mo.json(_exec.get("accumulators") or {}),
                        "Stage Step Timings": mo.ui.table(_stage2.get("steps", [])),
                    }
                ),
            ]
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Stage 2 failed", _exc)
    _out
    return


@app.cell
async def _(
    SUPPLIER_ID,
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_probe_fluss,
    mo,
    render_error_callout,
):
    mo.stop(not btn_probe_fluss.value)
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _obs = await _client.lookup_supplier(SUPPLIER_ID)
        if not _obs.get("found"):
            _out = mo.callout(
                mo.md(
                    f"✅ **Confirmed:** Native Fluss lookup for `{_obs.get('table')}` key `{_obs.get('key')}` "
                    f"returned `found=False` in `{_obs.get('duration_ms', 0):.1f}` ms. "
                    "Computed results currently live only in Kafka until Stage 3 ingests them."
                ),
                kind="info",
            )
        else:
            _out = mo.callout(
                mo.md(
                    f"ℹ️ Native Fluss lookup for `{_obs.get('table')}` key `{_obs.get('key')}` "
                    f"returned `found=True` (Stage 3 has already ingested this run)."
                ),
                kind="warn",
            )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Fluss pre-ingestion lookup failed", _exc)
    _out
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## User Input Cell: Flink SQL Workspace (`POST /api/sql/executions`)

        Write any permitted Flink SQL statement (`SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`, or
        `INSERT INTO computed_results`) over `business_events`, `playground_events`,
        `computed_results`, or `fluss_catalog`. Statements execute on the live Flink SQL Gateway.
        """
    )
    sql_mode = mo.ui.dropdown(
        options=["batch", "streaming"],
        value="batch",
        label="Execution Mode",
    )
    sql_token = mo.ui.text(value="", kind="password", label="Access Token (optional)")
    btn_sql_examples = mo.ui.run_button(label="Fetch Live SQL Examples")
    sql_editor = mo.ui.code_editor(
        value=(
            "SELECT event_type, COUNT(*) AS events,\n"
            "  MIN(occurred_at) AS first_at, MAX(occurred_at) AS last_at\n"
            "FROM business_events\n"
            "GROUP BY event_type"
        ),
        language="sql",
        min_height=180,
        label="Flink SQL Statement",
    )
    btn_run_sql = mo.ui.run_button(label="Execute Flink SQL")
    mo.vstack(
        [
            mo.hstack(
                [sql_mode, sql_token, btn_sql_examples], align="end", gap=1, wrap=True
            ),
            sql_editor,
            btn_run_sql,
        ]
    )
    return btn_run_sql, btn_sql_examples, sql_editor, sql_mode, sql_token


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_sql_examples,
    mo,
    render_error_callout,
):
    mo.stop(not btn_sql_examples.value)
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _examples = await _client.get_sql_examples()
        _sql_only = [
            {
                "id": ex.get("id"),
                "title": ex.get("title"),
                "mode": ex.get("mode"),
                "kind": ex.get("kind"),
                "statement": ex.get("statement"),
            }
            for ex in _examples
            if ex.get("workspace") == "sql"
        ]
        _out = mo.accordion(
            {
                f"Live SQL Workspace Examples ({len(_sql_only)} available)": mo.ui.table(
                    _sql_only
                )
            }
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Failed to load SQL examples", _exc)
    _out
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_run_sql,
    mo,
    render_error_callout,
    render_sql_result,
    sql_editor,
    sql_mode,
    sql_token,
):
    mo.stop(
        not btn_run_sql.value,
        mo.md(
            "### Execution Output\n*Click **Execute Flink SQL** above to display live Flink SQL results.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _exec = await _client.run_sql(
            statement=sql_editor.value,
            mode=sql_mode.value,
            workspace="sql",
            access_token=sql_token.value.strip() or None,
        )
        _view = render_sql_result(mo, _exec)
    except StreamhousePipelineError as _exc:
        _view = render_error_callout(mo, "Flink SQL execution failed", _exc)
    mo.vstack([mo.md("### Execution Output"), _view])
    return


if __name__ == "__main__":
    app.run()
