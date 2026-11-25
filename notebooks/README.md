# Streamhouse Marimo Notebooks

Interactive, reactive Python playbooks for the **Payment Risk Streamhouse Demo** (**Apache Kafka + Apache Flink + Apache Fluss**).

## Relationship to `web/v2.html` & `CONTRIBUTING.md`

Per [`CONTRIBUTING.md`](../CONTRIBUTING.md) Ground Rule 5, `web/v2.html` remains the default browser UI served by `demo-app` at `http://localhost:8090` (controlled by `DEMO_UI_VERSION`). These Marimo notebooks are an **optional Python developer playground** that drives the exact same `demo-app` REST API (`GET /api/state`, `POST /api/runs`, `POST /api/stages/{1..4}`, `GET /api/lookup/{table}`, `POST /api/events`, `POST /api/sql/executions`, `POST /api/python/executions`) without modifying or replacing `web/v2.html`.

All notebooks enforce the repository's ground rules:

1. **Never fake a pipeline step:** Zero synthetic fallback data. Every Kafka offset, Flink job ID, read-back proof, Fluss row, SQL result, and Python `stdout`/`stderr` comes from the live stack. If a service is stopped or unreachable, the output cell displays the real error banner. Nothing runs on page load.
2. **Stages are separate operations:** Every stage (Emit Kafka, Flink Processing, Fluss Ingestion, Fluss Query) and interactive input cell has its own `mo.ui.run_button` and polls `GET /api/state` until terminal state.
3. **Persistence is proven by read-back:** Stage 3 succeeds only when native Fluss lookups confirm every expected row (`read_back.verified == True` and `matched == checked > 0`).
4. **One scenario in the UI:** Stage buttons exclusively run the canonical Northstar Logistics scenario (`SUP-NORTHSTAR` / `PAY-NS-0001`).

## Prerequisites

Start the live Streamhouse stack from the repository root first:

```bash
docker compose up --build -d
```

Wait until `http://localhost:8090` reports Kafka, Flink, and Fluss as **READY**. To target a non-default port, set `STREAMHOUSE_APP_URL` (for example `STREAMHOUSE_APP_URL=http://localhost:18090`).

## Running the Notebooks

Using [`uv`](https://docs.astral.sh/uv/):

```bash
cd notebooks

# Install dependencies
uv sync --extra test

# Launch interactive multi-page app
uv run marimo run .

# Or edit an individual playbook
uv run marimo edit 00_simulation.py
```

## Playbooks & Interactive Input/Output Cells

* `00_simulation.py`: End-to-end 4-stage control plane with live cluster health inspection, run management, separate buttons for Stages 1–4, plus an **Interactive Execution Lab** (User Input cell supporting **Flink SQL**, **Kafka JSON Event**, or **Python `pyfluss`** + dedicated Execution Output cell).
* `01_kafka.py`: **Phase 1 (Kafka)** — Business event time vs processing time, `supplier_id` partition keying, `acks=all` acknowledgments, sequential offsets, plus a **Custom Kafka Event User Input & Output** section (`POST /api/events` + live templates).
* `02_flink.py`: **Phase 2 (Flink Processing)** — Bounded offset processing, `v1 -> v2 -> v3` context evolution, deterministic payment review rule, a read-only Fluss pre-ingestion probe, plus a **Flink SQL User Input & Output** section (`POST /api/sql/executions` + live examples).
* `03_fluss.py`: **Phase 3 & 4 (Fluss)** — Separate buttons for Stage 3 (bounded Flink ingestion + native read-back proof) and Stage 4 (native primary-key lookups), a direct `/api/lookup/{table}` inspector, plus dedicated **Fluss SQL** and **Python (`pyfluss`) User Input & Output** cells.

## Testing

Offline unit/compliance tests and live integration tests are explicitly separated:

```bash
cd notebooks

# Fast offline unit tests + static compliance checks (no Docker required)
uv run pytest -v -m "not integration"

# Marimo DAG validation and lint
uv run ruff check .
uv run marimo check 00_simulation.py 01_kafka.py 02_flink.py 03_fluss.py

# Genuine end-to-end integration tests (requires running Docker Compose stack)
uv run pytest -v -m integration tests/test_phases.py
```
