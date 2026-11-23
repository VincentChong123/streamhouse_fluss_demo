# Streamhouse Marimo Notebooks

Interactive, reactive Python playbooks for the **Payment Risk Streamhouse Demo** (Kafka + Flink + Fluss).

## Getting Started

Using `uv`:

```bash
# Sync dependencies
uv sync --extra test

# Run all notebooks with interactive multi-page navigation
uv run marimo run .

# Or edit a specific notebook
uv run marimo edit 01_kafka.py
```

## Notebooks

* `00_simulation.py`: Master end-to-end payment fraud simulation.
* `01_kafka.py`: Phase 1 — Kafka event ingestion and partition offsets.
* `02_flink.py`: Phase 2 — Bounded Flink stream processing and context evolution.
* `03_fluss.py`: Phase 3 — Fluss primary-key storage and native lookups.
