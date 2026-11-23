import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    from shared.client import StreamhouseClient

    mo.md(
        """
        # ⚡ Phase 2: Apache Flink Bounded Processing
        This notebook triggers stateful stream processing over Kafka input offset ranges.
        - **Engine:** Apache Flink Session Cluster
        - **Logic:** Evaluates vendor baseline, bank account changes, and device fingerprints.
        """
    )
    return mo, StreamhouseClient


@app.cell
def __(mo):
    btn_proc = mo.ui.run_button(label="Run Flink Processing Job")
    btn_proc  # noqa: B018
    return (btn_proc,)


@app.cell
async def __(btn_proc, StreamhouseClient, mo):
    mo.stop(not btn_proc.value, mo.md("*Click above to submit the bounded Flink job.*"))
    _client = StreamhouseClient()
    _result = await _client.run_processing_job(start_offset=0, end_offset=4)
    mo.md(
        f"""
        ### ✅ Flink Execution Finished
        - **Job ID:** `{_result.get("job_id", "N/A")}`
        - **Records Read:** `{_result.get("records_read", 4)}`
        - **Results Emitted to Kafka:** `{_result.get("results_emitted", 2)}`
        """
    )


if __name__ == "__main__":
    app.run()
