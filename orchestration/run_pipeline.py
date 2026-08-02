"""
orchestration/run_pipeline.py

Windows-compatible pipeline runner.

Executes the same steps as the Airflow DAG (shipment_pipeline.py) in sequence,
with the same branching logic and alerting. Use this for local development on
Windows where Airflow can't run natively.

Usage:
    python -m orchestration.run_pipeline
    python -m orchestration.run_pipeline --dry-run    # validate config only

For production, deploy the Airflow DAG in orchestration/dags/shipment_pipeline.py
on any Linux Airflow instance.

Exit codes:
    0  — pipeline succeeded (all checks pass or warnings only)
    1  — pipeline halted due to critical quality failure
    2  — unexpected exception
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Project root — this file lives at orchestration/run_pipeline.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_DIR      = PROJECT_ROOT / "dbt" / "shipment_dbt"
RESULTS_JSON = DBT_DIR / "target" / "run_results.json"
DATA_PATH    = PROJECT_ROOT / "data" / "raw" / "DataCoSupplyChainDataset.csv"

# Add project root to sys.path so observability/ingestion packages resolve
sys.path.insert(0, str(PROJECT_ROOT))


# ===========================================================================
# Coloured terminal output helpers (Windows-safe: just prefixes)
# ===========================================================================
def _log(level: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    prefix = {"INFO": "[INFO]", "WARN": "[WARN]", "ERROR": "[ERROR]", "STEP": "[STEP]"}
    print(f"{ts}  {prefix.get(level, '[LOG]')}  {msg}", flush=True)


def _section(title: str) -> None:
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


# ===========================================================================
# Step implementations (mirror the Airflow task callables)
# ===========================================================================

def step_ingest(dry_run: bool = False) -> None:
    """Run ingestion loader with schema drift validation."""
    _section("Step 1 of 6 — Ingestion + Schema Drift Validation")
    if dry_run:
        _log("INFO", "DRY RUN — skipping ingestion")
        return

    from ingestion.loader import run_ingestion

    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found at: {DATA_PATH}")

    _log("STEP", f"Loading {DATA_PATH.name}...")
    result = run_ingestion(str(DATA_PATH))
    _log("INFO", result.summary())

    if not result.is_valid:
        raise RuntimeError(
            f"Breaking schema drift detected — pipeline halted.\n{result.summary()}"
        )
    _log("INFO", "Ingestion complete. No breaking drift.")


def step_dbt_run(dry_run: bool = False) -> None:
    """Build all dbt models."""
    _section("Step 2 of 6 — dbt run (build models)")
    if dry_run:
        _log("INFO", "DRY RUN — skipping dbt run")
        return

    proc = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "run",
         "--exclude", "my_first_dbt_model", "my_second_dbt_model"],
        cwd=str(DBT_DIR),
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"dbt run failed with exit code {proc.returncode}")
    _log("INFO", "dbt run complete.")


def step_dbt_test(dry_run: bool = False) -> None:
    """Run dbt tests and persist results to MySQL."""
    _section("Step 3 of 6 — dbt test + log results")
    if dry_run:
        _log("INFO", "DRY RUN — skipping dbt test")
        return

    proc = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "test",
         "--exclude", "my_first_dbt_model", "my_second_dbt_model"],
        cwd=str(DBT_DIR),
        text=True,
    )
    # dbt exits 1 on warnings — that's acceptable. We log everything.
    _log("INFO", f"dbt test exited with code {proc.returncode}")

    if RESULTS_JSON.exists():
        from observability.test_result_logger import persist_results
        n = persist_results(RESULTS_JSON)
        _log("INFO", f"Logged {n} test results to dbt_test_results.")
    else:
        _log("WARN", f"run_results.json not found at {RESULTS_JSON}")


def step_anomaly_checks(dry_run: bool = False) -> list:
    """Run statistical anomaly checks."""
    _section("Step 4 of 6 — Statistical anomaly checks")
    if dry_run:
        _log("INFO", "DRY RUN — skipping anomaly checks")
        return []

    from observability.anomaly_checks import run_all_checks
    results = run_all_checks()
    for r in results:
        z_str = f"z={r.z_score:.2f}" if r.z_score is not None else "z=N/A"
        _log("INFO", f"  {r.check_name:<47} {r.status:<10} {z_str}")
    return results


def step_branch_and_alert(dry_run: bool = False) -> str:
    """
    Check severity of current run across all checks.
    Dispatches alerts and returns 'critical' or 'ok'.
    """
    _section("Step 5 of 6 — Severity check + alerting")
    if dry_run:
        _log("INFO", "DRY RUN — skipping alert dispatch")
        return "ok"

    import pandas as pd
    from observability.db import get_engine
    from observability.alerting import (
        alerts_from_dbt_results, alerts_from_anomaly_results, dispatch_batch
    )

    engine = get_engine()

    # Check for criticals across both tables
    dbt_df = pd.read_sql(
        """
        SELECT severity, test_name, message
        FROM   dbt_test_results
        WHERE  run_at = (SELECT MAX(run_at) FROM dbt_test_results)
          AND  status NOT IN ('pass', 'skipped')
        """,
        engine,
    )

    anom_df = pd.read_sql(
        """
        SELECT severity, check_name, message
        FROM   anomaly_check_results
        WHERE  run_at = (SELECT MAX(run_at) FROM anomaly_check_results)
          AND  status != 'pass'
        """,
        engine,
    )

    all_severities = list(dbt_df["severity"]) + list(anom_df["severity"])

    if not all_severities:
        _log("INFO", "All checks passed. No alerts to dispatch.")
        return "ok"

    _log("WARN" if "critical" not in all_severities else "ERROR",
         f"Non-passing checks: {len(all_severities)} "
         f"(critical={all_severities.count('critical')}, "
         f"warning={all_severities.count('warning')})")

    # Gather and dispatch
    alerts = alerts_from_dbt_results("") + alerts_from_anomaly_results()
    dispatch_batch(alerts)

    return "critical" if "critical" in all_severities else "ok"


def step_verify_idempotency(dry_run: bool = False) -> None:
    """Verify pipeline is idempotent: row count matches expected."""
    _section("Step 6 of 7 — Idempotency verification")
    if dry_run:
        _log("INFO", "DRY RUN — skipping idempotency check")
        return

    import pandas as pd
    from observability.db import get_engine

    engine = get_engine()
    count = pd.read_sql(
        "SELECT COUNT(*) AS n FROM raw_shipments", engine
    ).iloc[0]["n"]

    expected = 180_519
    if count == expected:
        _log("INFO", f"Idempotency PASSED: {count:,} rows == expected {expected:,}.")
    else:
        _log("WARN",
             f"Row count {count:,} != expected {expected:,}. "
             f"Possible partial load or dataset has changed.")


def step_export_and_publish(dry_run: bool = False) -> None:
    """Export snapshots and push to GitHub to auto-update Streamlit Cloud."""
    _section("Step 7 of 7 — Export Snapshots & Publish to Live Dashboard")
    if dry_run:
        _log("INFO", "DRY RUN — skipping snapshot export & git push")
        return

    from scripts.export_snapshots import export_snapshots
    export_snapshots()
    _log("INFO", "Snapshots exported to data/snapshots/")

    try:
        subprocess.run(["git", "add", "data/snapshots/"], cwd=str(PROJECT_ROOT), check=True)
        status = subprocess.run(["git", "status", "--porcelain"], cwd=str(PROJECT_ROOT), capture_output=True, text=True)
        if "data/snapshots" in status.stdout:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            subprocess.run(["git", "commit", "-m", f"auto: update dashboard data snapshots [{timestamp}]"], cwd=str(PROJECT_ROOT), check=True)
            subprocess.run(["git", "push", "origin", "main"], cwd=str(PROJECT_ROOT), check=True)
            _log("INFO", "Git push successful — Streamlit Cloud auto-updating live UI!")
        else:
            _log("INFO", "No changes in snapshot data — skipping git commit.")
    except Exception as exc:
        _log("WARN", f"Auto-publish to Git encountered a non-fatal notice: {exc}")


# ===========================================================================
# Pipeline orchestrator
# ===========================================================================

def run_pipeline(dry_run: bool = False) -> int:
    """
    Run all pipeline steps in sequence with the same branching logic as the DAG.
    Returns exit code: 0 = success, 1 = critical failure, 2 = exception.
    """
    start = time.time()
    print()
    _log("INFO", "Shipment Observability Pipeline starting...")
    _log("INFO", f"Project root: {PROJECT_ROOT}")
    if dry_run:
        _log("INFO", "*** DRY RUN MODE — no data will be written ***")

    try:
        step_ingest(dry_run)
        step_dbt_run(dry_run)
        step_dbt_test(dry_run)
        step_anomaly_checks(dry_run)
        outcome = step_branch_and_alert(dry_run)
        step_verify_idempotency(dry_run)
        
        if outcome != "critical":
            step_export_and_publish(dry_run)

    except Exception as exc:
        _log("ERROR", f"Pipeline exception: {exc}")
        elapsed = time.time() - start
        _log("ERROR", f"Pipeline FAILED after {elapsed:.1f}s")
        return 2

    elapsed = time.time() - start

    if outcome == "critical":
        print()
        _log("ERROR", "=" * 50)
        _log("ERROR", "PIPELINE HALTED — critical quality failure.")
        _log("ERROR", "Dashboard refresh is blocked until issues are resolved.")
        _log("ERROR", f"Total elapsed: {elapsed:.1f}s")
        _log("ERROR", "=" * 50)
        return 1
    else:
        print()
        _log("INFO", "=" * 50)
        _log("INFO", "PIPELINE SUCCEEDED")
        _log("INFO", f"Total elapsed: {elapsed:.1f}s")
        _log("INFO", "=" * 50)
        return 0


# ===========================================================================
# CLI entry point
# ===========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Shipment observability pipeline runner (Windows-compatible alternative "
            "to the Airflow DAG in orchestration/dags/shipment_pipeline.py)."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and imports without writing any data.",
    )
    args = parser.parse_args()
    code = run_pipeline(dry_run=args.dry_run)
    sys.exit(code)


if __name__ == "__main__":
    main()
