"""
observability/test_result_logger.py

Parses dbt's run_results.json (written after every `dbt test` run) and
persists each test result as a row in the `dbt_test_results` table.

Why this matters:
    dbt's native `dbt test` command only shows pass/fail in the terminal.
    Once the run finishes, the history is gone. By persisting results to a
    table, we convert dbt tests from a one-time gate into a TIME SERIES —
    letting us ask questions like "has this test been flaky over the last
    30 runs?" or "did the null rate on shipping_cost start drifting this week?"
    That's the foundation the observability dashboard builds on.

Usage:
    python -m observability.test_result_logger \\
        --results-path dbt/shipment_dbt/target/run_results.json

Or import and call programmatically from an Airflow task.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from observability.db import get_engine, ensure_observability_tables

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

# ---------------------------------------------------------------------------
# Severity mapping
#
# The dbt-mysql adapter stores status as 'pass', 'warn', 'error', 'skipped'.
# We map each to a severity tier so the orchestration layer can make
# branching decisions (critical = halt, warning = alert-and-continue, info = log).
# ---------------------------------------------------------------------------
_STATUS_TO_SEVERITY: dict[str, str] = {
    "pass":    "info",
    "warn":    "warning",
    "error":   "critical",
    "skipped": "info",
    "fail":    "critical",   # older dbt versions use 'fail' instead of 'error'
}

# Some tests we know are "expected warnings" — e.g., late_flag_consistency fires
# on real DataCo data quality debt, not a pipeline bug. We can downgrade them.
_KNOWN_WARNINGS: set[str] = {
    "assert_late_flag_consistency",
}


def _severity(status: str, test_name: str) -> str:
    """Return severity string, downgrading known-safe warnings to 'warning'."""
    base = _STATUS_TO_SEVERITY.get(status, "critical")
    if base == "critical" and test_name in _KNOWN_WARNINGS:
        return "warning"
    return base


def _extract_model_and_column(test_unique_id: str) -> tuple[str, str]:
    """
    Parse test unique ID like:
      'test.shipment_dbt.not_null_fact_shipments_order_id.abc123'
    into (model_name='fact_shipments', column_name='order_id').
    Returns ('', '') when parsing fails (e.g., custom singular tests).
    """
    parts = test_unique_id.split(".")
    if len(parts) < 3:
        return "", ""
    test_short_name = parts[2]   # e.g., 'not_null_fact_shipments_order_id'
    # strip known prefixes from dbt generic test names
    for prefix in ("not_null_", "unique_", "accepted_values_", "relationships_"):
        if test_short_name.startswith(prefix):
            remainder = test_short_name[len(prefix):]
            # remainder is now 'fact_shipments_order_id'
            # The model name is the first two underscore-joined tokens
            tokens = remainder.split("_")
            model = "_".join(tokens[:2])
            column = "_".join(tokens[2:]) if len(tokens) > 2 else ""
            return model, column
    # Custom singular tests — no embedded model/column in the name
    return "", ""


def load_run_results(results_path: Path) -> dict[str, Any]:
    """Load and return the parsed run_results.json content."""
    with results_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def persist_results(results_path: Path) -> int:
    """
    Read run_results.json and write every test result to dbt_test_results.

    Returns the number of rows inserted.
    """
    data = load_run_results(results_path)
    engine = get_engine()
    ensure_observability_tables(engine)

    metadata = data.get("metadata", {})
    run_id   = metadata.get("invocation_id", "unknown")
    # dbt stores generated_at in ISO format, e.g. '2026-08-02T20:07:27.123456Z'
    generated_at_str = metadata.get("generated_at", "")
    try:
        run_at = datetime.fromisoformat(generated_at_str.replace("Z", "+00:00"))
        run_at = run_at.astimezone(timezone.utc).replace(tzinfo=None)  # store as UTC naive
    except (ValueError, AttributeError):
        run_at = datetime.utcnow()

    results = data.get("results", [])
    if not results:
        logger.warning("No results found in %s — nothing to insert.", results_path)
        return 0

    rows = []
    for r in results:
        unique_id     = r.get("unique_id", "")
        status        = r.get("status", "unknown")
        execution_time = r.get("execution_time", None)
        failures      = r.get("failures", 0) or 0
        message       = r.get("message", "") or ""

        # Extract the short test name (last segment of unique_id)
        test_name = unique_id.split(".")[-1] if unique_id else "unknown"
        model_name, column_name = _extract_model_and_column(unique_id)
        severity = _severity(status, test_name)

        # Path to the compiled SQL — useful for debugging failures in the dashboard
        compiled_sql_path = str(
            results_path.parent / "compiled" / "shipment_dbt" / "tests" / f"{test_name}.sql"
        )

        rows.append({
            "run_id":            run_id,
            "run_at":            run_at,
            "test_name":         test_name,
            "model_name":        model_name,
            "column_name":       column_name,
            "status":            status,
            "severity":          severity,
            "failure_count":     int(failures),
            "execution_time_s":  execution_time,
            "message":           message[:1000] if message else "",
            "compiled_sql_path": compiled_sql_path,
        })

    insert_sql = text("""
        INSERT INTO dbt_test_results
            (run_id, run_at, test_name, model_name, column_name,
             status, severity, failure_count, execution_time_s, message, compiled_sql_path)
        VALUES
            (:run_id, :run_at, :test_name, :model_name, :column_name,
             :status, :severity, :failure_count, :execution_time_s, :message, :compiled_sql_path)
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    logger.info(
        "Inserted %d test results from run %s (run_at=%s)",
        len(rows), run_id, run_at.isoformat()
    )
    return len(rows)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse dbt run_results.json and persist results to MySQL."
    )
    parser.add_argument(
        "--results-path",
        required=True,
        help="Path to dbt's run_results.json (e.g., dbt/shipment_dbt/target/run_results.json)",
    )
    args = parser.parse_args()
    path = Path(args.results_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"run_results.json not found at: {path}")
    n = persist_results(path)
    print(f"Logged {n} test results to dbt_test_results.")


if __name__ == "__main__":
    main()
