import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    from shared.client import StreamhouseClient, StreamhousePipelineError
    from shared.render import (
        render_error_callout,
        render_python_result,
        render_sql_result,
    )
    from shared.scenario import FLUSS_TABLES, PAYMENT_ID, SUPPLIER_ID

    mo.md(
        """
        # Phase 3 & 4: Apache Fluss Ingestion, Read-Back & Native Lookups

        This playbook separates **Stage 3 (Ingestion + Read-Back Proof)** and
        **Stage 4 (Native Primary-Key Lookups)** per `CONTRIBUTING.md` Ground Rules 2 and 3:

        - **Stage 3 (Bounded Flink Ingestion):** Validates `computed-result/v1` records in Kafka
          `computed-results`, submits a bounded Flink ingestion job writing to Fluss primary-key
          tables (`supplier_risk`, `payment_reviews`, `event_receipts`), and succeeds **only** when
          native Fluss point lookups confirm every expected row.
        - **Stage 4 (Native Point Lookups):** Executes concurrent native primary-key lookups against
          Fluss (`Table.newLookup().createLookuper().lookup(primary key)`) — no SQL, no cache.
        """
    )
    return (
        FLUSS_TABLES,
        PAYMENT_ID,
        SUPPLIER_ID,
        StreamhouseClient,
        StreamhousePipelineError,
        mo,
        render_error_callout,
        render_python_result,
        render_sql_result,
    )


@app.cell
def _(FLUSS_TABLES, mo):
    _schema_rows = [
        {
            "table": f"payment_risk.{name}",
            "primary_key": ", ".join(meta["primary_key"]),
            "contents": meta["description"],
        }
        for name, meta in FLUSS_TABLES.items()
    ]
    mo.accordion({"Fluss Primary-Key Table Contracts": mo.ui.table(_schema_rows)})
    return


@app.cell
def _(mo):
    app_url = mo.ui.text(value="http://localhost:8090", label="demo-app URL")
    btn_ingest = mo.ui.run_button(label="Stage 3: Ingest Results into Fluss")
    btn_query = mo.ui.run_button(label="Stage 4: Query Fluss (Native Lookups)")
    mo.hstack([app_url, btn_ingest, btn_query], align="end", gap=1, wrap=True)
    return app_url, btn_ingest, btn_query


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_ingest,
    mo,
    render_error_callout,
):
    mo.stop(
        not btn_ingest.value,
        mo.md(
            "*Click **Stage 3: Ingest Results into Fluss** above to run the bounded ingestion job.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _stage3 = await _client.run_ingestion_job()
        _exec = _stage3["execution"]
        _rb = _exec["read_back"]
        _job_id = _exec.get("job_id")
        _job_url = _exec.get("job_url")
        _job_link = f"[`{_job_id}`]({_job_url})" if _job_url else f"`{_job_id}`"

        _rb_rows = [
            {
                "table": row.get("table"),
                "primary_key": str(row.get("key")),
                "matches": row.get("matches"),
                "found": (row.get("observation") or {}).get("found"),
                "lookup_ms": round(
                    (row.get("observation") or {}).get("duration_ms", 0.0), 2
                ),
                "computed_by": row.get("computed_by"),
                "mismatches": ", ".join(row.get("mismatches") or []),
            }
            for row in _rb.get("rows", [])
        ]
        _dest_rows = [
            {
                "table": d.get("table"),
                "primary_key": ", ".join(d.get("primary_key") or []),
                "rows_expected": d.get("rows_expected"),
            }
            for d in _exec.get("destinations", [])
        ]

        _out = mo.vstack(
            [
                mo.callout(
                    mo.md(
                        f"### ✅ Stage 3 Fluss Ingestion & Read-Back SUCCEEDED\n"
                        f"- **Run ID:** `{_stage3['run_id']}`\n"
                        f"- **Ingestion Job ID:** {_job_link} (`{_exec.get('job_state')}`)\n"
                        f"- **Source Range:** `{_exec.get('source_topic')}` "
                        f"`[{_exec.get('source_start')}, {_exec.get('source_end')})` "
                        f"(`{_exec.get('records_valid')}` valid, `{len(_exec.get('rejected', []))}` rejected)\n"
                        f"- **Read-Back Proof:** `{_rb['matched']}/{_rb['checked']}` rows confirmed "
                        f"(`verified={_rb['verified']}` via `{_rb.get('method')}`)\n"
                        f"- **Stage Duration:** `{_stage3.get('duration_ms')}` ms"
                    ),
                    kind="success",
                ),
                mo.ui.table(
                    _rb_rows, label="Native Fluss Read-Back Verification (Every Row)"
                ),
                mo.accordion(
                    {
                        "Destination Tables": mo.ui.table(_dest_rows),
                        "Stage Step Timings": mo.ui.table(_stage3.get("steps", [])),
                    }
                ),
            ]
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Stage 3 failed", _exc)
    _out
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_query,
    mo,
    render_error_callout,
):
    mo.stop(
        not btn_query.value,
        mo.md(
            "*Click **Stage 4: Query Fluss (Native Lookups)** after Stage 3 succeeds.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _stage4 = await _client.query_fluss_stage()
        _res = _stage4["result"]
        _sup = _res["supplier"]
        _rev = _res["review"]
        _sup_row = _sup.get("row") or {}
        _rev_row = _rev.get("row") or {}

        _summary = [
            {
                "table": _sup.get("table"),
                "key": str(_sup.get("key")),
                "lookup_ms": round(_sup.get("duration_ms", 0.0), 2),
                "highlights": (
                    f"context_version=v{_sup_row.get('context_version')}, "
                    f"signal_band={_sup_row.get('signal_band')}, "
                    f"unfamiliar_device={_sup_row.get('unfamiliar_device_evidence')}, "
                    f"beneficiary={_sup_row.get('current_beneficiary_masked')}"
                ),
            },
            {
                "table": _rev.get("table"),
                "key": str(_rev.get("key")),
                "lookup_ms": round(_rev.get("duration_ms", 0.0), 2),
                "highlights": (
                    f"outcome={_rev_row.get('outcome')}, "
                    f"recent_change={_rev_row.get('check_recent_change')}, "
                    f"unfamiliar_device={_rev_row.get('check_unfamiliar_device')}, "
                    f"large_payment={_rev_row.get('check_large_payment')}"
                ),
            },
        ]

        _out = mo.vstack(
            [
                mo.callout(
                    mo.md(
                        f"### ✅ Stage 4 Native Fluss Lookups SUCCEEDED\n"
                        f"- **Run ID:** `{_stage4['run_id']}`\n"
                        f"- **Mechanism:** `{_res.get('mechanism')}`\n"
                        f"- **Queried At:** `{_res.get('queried_at')}`\n"
                        f"- **Deterministic Explanation:** {_rev_row.get('explanation', '')}"
                    ),
                    kind="success",
                ),
                mo.ui.table(_summary, label="Native Primary-Key Lookup Results"),
                mo.accordion(
                    {
                        "Raw Fluss Observation (supplier_risk)": mo.json(_sup),
                        "Raw Fluss Observation (payment_reviews)": mo.json(_rev),
                    }
                ),
            ]
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Stage 4 failed", _exc)
    _out
    return


@app.cell
def _(PAYMENT_ID, SUPPLIER_ID, mo):
    mo.md("## Direct Point-Lookup Inspector (`GET /api/lookup/{table}`)")
    table_select = mo.ui.dropdown(
        options=["supplier_risk", "payment_reviews"],
        value="supplier_risk",
        label="Fluss Table",
    )
    entity_input = mo.ui.text(
        value=SUPPLIER_ID,
        label=f"Entity ID (default: {SUPPLIER_ID} or {PAYMENT_ID})",
    )
    run_id_input = mo.ui.text(
        value="",
        label="Run ID (blank = active run from /api/state)",
    )
    btn_direct_lookup = mo.ui.run_button(label="Run Direct Point Lookup")
    mo.hstack(
        [table_select, entity_input, run_id_input, btn_direct_lookup],
        align="end",
        gap=1,
        wrap=True,
    )
    return btn_direct_lookup, entity_input, run_id_input, table_select


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_direct_lookup,
    entity_input,
    mo,
    render_error_callout,
    run_id_input,
    table_select,
):
    mo.stop(not btn_direct_lookup.value)
    _client = StreamhouseClient(app_url=app_url.value)
    _run_override = run_id_input.value.strip() or None
    try:
        if table_select.value == "supplier_risk":
            _obs = await _client.lookup_supplier(
                supplier_id=entity_input.value.strip(),
                run_id=_run_override,
            )
        else:
            _obs = await _client.lookup_payment_review(
                payment_id=entity_input.value.strip(),
                run_id=_run_override,
            )

        if _obs.get("found"):
            _out = mo.vstack(
                [
                    mo.callout(
                        mo.md(
                            f"✅ Found row in `{_obs.get('table')}` for `{_obs.get('key')}` "
                            f"in `{_obs.get('duration_ms', 0):.2f}` ms"
                        ),
                        kind="success",
                    ),
                    mo.json(_obs),
                ]
            )
        else:
            _out = mo.callout(
                mo.md(
                    f"ℹ️ No row in `{_obs.get('table')}` for `{_obs.get('key')}` "
                    f"(`found=False`, lookup `{_obs.get('duration_ms', 0):.2f}` ms). "
                    "Run Stage 3 ingestion first if this is a fresh run."
                ),
                kind="warn",
            )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Direct lookup failed", _exc)
    _out
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## User Input Cell 1: Query Fluss with Flink SQL (`POST /api/sql/executions`)

        Execute Flink SQL over `fluss_catalog.payment_risk.*` tables (`supplier_risk`,
        `payment_reviews`, `event_receipts`).
        """
    )
    fluss_sql_mode = mo.ui.dropdown(
        options=["batch", "streaming"],
        value="batch",
        label="Execution Mode",
    )
    workspace_token = mo.ui.text(
        value="", kind="password", label="Access Token (optional)"
    )
    fluss_sql_editor = mo.ui.code_editor(
        value=(
            "SELECT outcome, COUNT(*) AS reviews\n"
            "FROM fluss_catalog.payment_risk.payment_reviews\n"
            "GROUP BY outcome"
        ),
        language="sql",
        min_height=140,
        label="Fluss SQL Query",
    )
    btn_run_fluss_sql = mo.ui.run_button(label="Execute Fluss SQL Query")
    mo.vstack(
        [
            mo.hstack([fluss_sql_mode, workspace_token], align="end", gap=1, wrap=True),
            fluss_sql_editor,
            btn_run_fluss_sql,
        ]
    )
    return btn_run_fluss_sql, fluss_sql_editor, fluss_sql_mode, workspace_token


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_run_fluss_sql,
    fluss_sql_editor,
    fluss_sql_mode,
    mo,
    render_error_callout,
    render_sql_result,
    workspace_token,
):
    mo.stop(
        not btn_run_fluss_sql.value,
        mo.md(
            "### Fluss SQL Execution Output\n*Click **Execute Fluss SQL Query** above to display live results.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _exec = await _client.run_sql(
            statement=fluss_sql_editor.value,
            mode=fluss_sql_mode.value,
            workspace="query",
            access_token=workspace_token.value.strip() or None,
        )
        _view = render_sql_result(mo, _exec)
    except StreamhousePipelineError as _exc:
        _view = render_error_callout(mo, "Fluss SQL failed", _exc)
    mo.vstack([mo.md("### Fluss SQL Execution Output"), _view])
    return


@app.cell
def _(mo):
    mo.md(
        """
        ---
        ## User Input Cell 2: Python (`pyfluss`) Playground (`POST /api/python/executions`)

        Run Python code directly against Apache Fluss inside the isolated `python-runner`
        container (`pyfluss 1.0.0`). Click **Fetch Current Run Example** to view the server's
        ready-to-run template populated with the active `run_id`.
        """
    )
    btn_py_example = mo.ui.run_button(label="Fetch Current Run Example")
    python_editor = mo.ui.code_editor(
        value=(
            "import asyncio, os\n"
            "import fluss\n\n"
            "async def main():\n"
            '    config = fluss.Config({"bootstrap.servers": os.environ["FLUSS_BOOTSTRAP_SERVERS"]})\n'
            "    conn = await fluss.FlussConnection.create(config)\n"
            "    try:\n"
            "        admin = await conn.get_admin()\n"
            '        print("Databases:", await admin.list_databases())\n'
            '        print("Tables in payment_risk:", await admin.list_tables("payment_risk"))\n'
            "    finally:\n"
            "        await conn.close()\n\n"
            "asyncio.run(main())\n"
        ),
        language="python",
        min_height=220,
        label="Python (pyfluss) Code",
    )
    btn_run_python = mo.ui.run_button(label="Execute Python (pyfluss)")
    mo.vstack([btn_py_example, python_editor, btn_run_python])
    return btn_py_example, btn_run_python, python_editor


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_py_example,
    mo,
    render_error_callout,
):
    mo.stop(not btn_py_example.value)
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _code = await _client.get_python_example()
        _out = mo.accordion(
            {
                "Current Run pyfluss Example (copy into the editor above)": mo.md(
                    f"```python\n{_code}\n```"
                )
            }
        )
    except StreamhousePipelineError as _exc:
        _out = render_error_callout(mo, "Failed to fetch Python example", _exc)
    _out
    return


@app.cell
async def _(
    StreamhouseClient,
    StreamhousePipelineError,
    app_url,
    btn_run_python,
    mo,
    python_editor,
    render_error_callout,
    render_python_result,
    workspace_token,
):
    mo.stop(
        not btn_run_python.value,
        mo.md(
            "### Python Execution Output\n*Click **Execute Python (pyfluss)** above to see `stdout`, `stderr`, exit code, and timing.*"
        ),
    )
    _client = StreamhouseClient(app_url=app_url.value)
    try:
        _exec = await _client.run_python(
            code=python_editor.value,
            access_token=workspace_token.value.strip() or None,
        )
        _view = render_python_result(mo, _exec)
    except StreamhousePipelineError as _exc:
        _view = render_error_callout(mo, "Python execution failed", _exc)
    mo.vstack([mo.md("### Python Execution Output"), _view])
    return


@app.cell
def _(mo):
    mo.accordion(
        {
            "Architecture Notes: AI Anomaly Detection & Observability Patterns": mo.md(
                """
                Once Fluss maintains fresh primary-key state (`supplier_risk`, `payment_reviews`),
                downstream Python services or AI agents can combine sub-second Kafka triggers with
                sub-millisecond `pyfluss` point lookups:

                - **Metrics & State Timelines (Grafana / Prometheus):** Consumer lag, throughput,
                  Flink checkpoint duration, and vertical event annotations for `BENEFICIARY_CHANGED`
                  and `PAYMENT_REQUESTED`.
                - **Distributed Traces (HyperDX / OpenTelemetry):** End-to-end span waterfall across
                  `Producer -> Kafka -> Flink Operator -> Fluss Ingestion -> Consumer`.
                - **AI Agent Traces (Langfuse):** Captures tool calls (`lookup_supplier_profile`),
                  Fluss context returned, reasoning steps, and escalation decisions without replacing
                  the deterministic payment rule.
                """
            )
        }
    )
    return


if __name__ == "__main__":
    app.run()
