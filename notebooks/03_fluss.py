import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    from shared.client import StreamhouseClient

    mo.md(
        """
        # 🌊 Phase 3: Apache Fluss Storage & Lookups
        This notebook ingests computed results into Fluss and performs sub-millisecond point lookups.
        - **Storage:** Apache Fluss Primary-Key Tables
        - **Access:** Direct Key-Value Lookup
        """
    )
    return mo, StreamhouseClient


@app.cell
def __(mo):
    btn_query = mo.ui.run_button(label="Query Fluss for Supplier State")
    btn_query  # noqa: B018
    return (btn_query,)


@app.cell
async def __(btn_query, StreamhouseClient, mo):
    mo.stop(
        not btn_query.value,
        mo.md("*Click above to run the point lookup against Fluss.*"),
    )
    _client = StreamhouseClient()
    _row = await _client.lookup_supplier("SUP-NORTHSTAR")
    mo.json(_row)


if __name__ == "__main__":
    app.run()
