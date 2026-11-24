"""Marimo output renderers for interactive Kafka, Flink SQL, and Python executions.

Every renderer displays only real values returned by ``demo-app`` (never synthetic
placeholders or fallback counts).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utc_timestamp() -> str:
    """Return the current UTC timestamp in ISO-8601 format (``YYYY-MM-DDTHH:MM:SSZ``)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_error_message(
    label: str,
    error: Any,
    *,
    timestamp: str | None = None,
) -> str:
    """Format a markdown error line with an ISO-8601 UTC timestamp."""
    ts = timestamp or getattr(error, "timestamp", None) or utc_timestamp()
    return f"❌ `[{ts}]` **{label}:** {error}"


def render_error_callout(
    mo: Any,
    label: str,
    error: Any,
    *,
    timestamp: str | None = None,
) -> Any:
    """Render a danger Marimo callout with an ISO-8601 UTC timestamp."""
    return mo.callout(
        mo.md(format_error_message(label, error, timestamp=timestamp)),
        kind="danger",
    )


def render_custom_events_result(mo: Any, response: dict[str, Any]) -> Any:
    """Render live Kafka broker acknowledgments returned by ``POST /api/events``."""
    records = response.get("records") or []
    rows = [
        {
            "id": rec.get("id"),
            "topic": rec.get("topic"),
            "key": rec.get("key") or "",
            "status": rec.get("status"),
            "partition": rec.get("partition"),
            "offset": rec.get("offset"),
            "send_ms": rec.get("send_ms"),
            "kafka_timestamp": rec.get("kafka_timestamp"),
            "warning": rec.get("warning") or "",
            "error": rec.get("error") or "",
        }
        for rec in records
    ]
    all_ok = bool(records) and all(
        rec.get("status") == "ACKNOWLEDGED" for rec in records
    )
    kind = "success" if all_ok else "danger"
    icon = "✅" if all_ok else "❌"
    ts_prefix = "" if all_ok else f"`[{utc_timestamp()}]` "
    headline = (
        f"{icon} {ts_prefix}**Kafka Producer Result:** `{len(rows)}` record(s) processed"
        + (f" — {response['error']}" if response.get("error") else "")
    )
    parts = [
        mo.callout(mo.md(headline), kind=kind),
        mo.ui.table(rows, label="Kafka Broker Acknowledgments"),
        mo.accordion({"Raw Response": mo.json(response)}),
    ]
    return mo.vstack(parts)


def render_sql_result(mo: Any, execution: dict[str, Any]) -> Any:
    """Render a terminal Flink SQL Gateway execution from ``/api/state``."""
    status = execution.get("status") or "UNKNOWN"
    kind_badge = (
        "success"
        if status == "FINISHED"
        else "warn"
        if status == "CANCELED"
        else "danger"
    )
    icon = "✅" if status == "FINISHED" else "⚠️" if status == "CANCELED" else "❌"
    err_ts = (
        execution.get("finished_at")
        or execution.get("updated_at")
        or utc_timestamp()
    )
    ts_prefix = f"`[{err_ts}]` " if icon == "❌" else ""

    job_id = execution.get("job_id")
    job_url = execution.get("job_url")
    job_text = (
        f"[`{job_id}`]({job_url})"
        if (job_id and job_url)
        else f"`{job_id}`"
        if job_id
        else "`none`"
    )

    summary_lines = [
        f"### {icon} {ts_prefix}Flink SQL Execution `{status}`",
        f"- **Execution ID:** `{execution.get('id')}` · **Kind:** `{execution.get('kind')}` · **Mode:** `{execution.get('mode')}`",
        f"- **Flink Job:** {job_text} (`{execution.get('job_state') or 'N/A'}`)",
        f"- **Duration:** `{execution.get('duration_ms')}` ms · **Rows:** `{execution.get('row_count', 0)}`"
        + (" *(truncated at server limit)*" if execution.get("truncated") else ""),
    ]
    if execution.get("note"):
        summary_lines.append(f"- **Note:** {execution['note']}")
    if execution.get("error"):
        summary_lines.append(f"- **Error** (`{err_ts}`): {execution['error']}")

    parts: list[Any] = [mo.callout(mo.md("\n".join(summary_lines)), kind=kind_badge)]

    if execution.get("error_detail"):
        parts.append(
            mo.accordion(
                {
                    "Flink Error Detail": mo.md(
                        f"```text\n{execution['error_detail']}\n```"
                    )
                }
            )
        )

    columns = execution.get("columns") or []
    raw_rows = execution.get("rows") or []
    if raw_rows:
        col_names = [c.get("name") or f"col_{idx}" for idx, c in enumerate(columns)]
        table_rows = []
        for entry in raw_rows:
            fields = entry.get("fields") if isinstance(entry, dict) else entry
            if not isinstance(fields, list):
                fields = [fields]
            row_dict: dict[str, Any] = {}
            if (
                execution.get("mode") == "streaming"
                and isinstance(entry, dict)
                and entry.get("kind")
            ):
                row_dict["_op"] = entry["kind"]
            for idx, val in enumerate(fields):
                name = col_names[idx] if idx < len(col_names) else f"col_{idx}"
                row_dict[name] = val
            table_rows.append(row_dict)
        parts.append(mo.ui.table(table_rows, label="SQL Query Result Rows"))
    elif status == "FINISHED" and execution.get("kind") != "INSERT":
        parts.append(mo.md("*Query finished with 0 rows returned.*"))

    insert_out = execution.get("output")
    if isinstance(insert_out, dict):
        parts.append(
            mo.md(
                f"**INSERT Output (`{insert_out.get('topic')}`):** "
                f"offsets `[{insert_out.get('start')}, {insert_out.get('end')})` · "
                f"`{insert_out.get('records')}` record(s) (`{insert_out.get('valid')}` valid, "
                f"`{insert_out.get('invalid')}` invalid)"
            )
        )
        if insert_out.get("first_errors"):
            parts.append(mo.json(insert_out["first_errors"]))

    parts.append(mo.accordion({"Raw SQL Execution Record": mo.json(execution)}))
    return mo.vstack(parts)


def render_python_result(mo: Any, execution: dict[str, Any]) -> Any:
    """Render a terminal Python (``pyfluss``) execution from ``/api/state``."""
    status = execution.get("status") or "UNKNOWN"
    kind_badge = (
        "success"
        if status == "FINISHED"
        else "warn"
        if status == "CANCELED"
        else "danger"
    )
    icon = "✅" if status == "FINISHED" else "⚠️" if status == "CANCELED" else "❌"
    err_ts = (
        execution.get("finished_at")
        or execution.get("updated_at")
        or utc_timestamp()
    )
    ts_prefix = f"`[{err_ts}]` " if icon == "❌" else ""

    summary_lines = [
        f"### {icon} {ts_prefix}Python (`pyfluss`) Execution `{status}`",
        f"- **Execution ID:** `{execution.get('id')}` · **Exit Code:** `{execution.get('exit_code')}` · **Duration:** `{execution.get('duration_ms')}` ms",
        f"- **Runtime:** `{execution.get('client') or 'python-runner'}`",
    ]
    if execution.get("truncated"):
        summary_lines.append(
            "- **Warning:** Output was truncated at the runner's 64 KiB cap."
        )
    if execution.get("error"):
        summary_lines.append(f"- **Error** (`{err_ts}`): {execution['error']}")

    parts: list[Any] = [mo.callout(mo.md("\n".join(summary_lines)), kind=kind_badge)]

    stdout = execution.get("stdout") or ""
    stderr = execution.get("stderr") or ""

    parts.append(
        mo.md(
            "#### `stdout`\n"
            + (f"```text\n{stdout}\n```" if stdout.strip() else "*`(empty)`*")
        )
    )
    if stderr.strip():
        parts.append(mo.md(f"#### `stderr`\n```text\n{stderr}\n```"))

    parts.append(mo.accordion({"Raw Python Execution Record": mo.json(execution)}))
    return mo.vstack(parts)
