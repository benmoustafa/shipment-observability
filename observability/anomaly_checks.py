"""
observability/anomaly_checks.py

Statistical anomaly detection layer.

Three checks, each operating on a rolling 30-run window:
  1. row_count_anomaly    — today's row count vs rolling mean/stddev
  2. metric_drift         — average of a numeric metric vs trailing distribution
  3. null_rate_drift      — null % in a key column vs historical baseline

Why statistical checks on top of dbt tests?
  dbt tests are STATIC: "is_late must be 0 or 1", "order_id must not be null".
  They catch structural breakage, not subtle shifts. A static test won't catch:
    - "We loaded 40% fewer rows than usual today" (row count looks valid, just low)
    - "Average shipping_cost jumped 30% — upstream changed a fee structure"
    - "Null rate on customer_email went from 0.2% to 8% this week"
  Statistical checks catch these. Together, static + statistical = a defence-in-depth
  quality strategy that's genuinely different from most portfolio projects.

Algorithm: z-score against rolling window
  z = (today_value - window_mean) / window_stddev
  |z| > threshold (default 3.0) → anomaly flag
  This is intentionally simple and explainable — the point is demonstrating
  understanding, not building a research-grade detector.

Usage:
    python -m observability.anomaly_checks

Or call run_all_checks() programmatically from an Airflow task.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

import pandas as pd
from sqlalchemy import text

from observability.db import get_engine, ensure_observability_tables

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WINDOW_SIZE       = 30     # rolling window: last N check runs
Z_THRESHOLD_WARN  = 2.0    # |z| > 2  → warning
Z_THRESHOLD_CRIT  = 3.0    # |z| > 3  → critical


# ---------------------------------------------------------------------------
# Data model for a single check result
# ---------------------------------------------------------------------------
@dataclass
class CheckResult:
    check_name:     str
    table_name:     str
    column_name:    str
    status:         str          # 'pass' | 'warn' | 'critical'
    severity:       str          # 'info' | 'warning' | 'critical'
    observed_value: Optional[float]
    expected_value: Optional[float]
    z_score:        Optional[float]
    threshold:      float
    message:        str
    run_at:         datetime = field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Helper: classify by z-score
# ---------------------------------------------------------------------------
def _classify(z: float) -> tuple[str, str]:
    """Return (status, severity) based on absolute z-score."""
    abs_z = abs(z)
    if abs_z > Z_THRESHOLD_CRIT:
        return "critical", "critical"
    elif abs_z > Z_THRESHOLD_WARN:
        return "warn", "warning"
    else:
        return "pass", "info"


# ---------------------------------------------------------------------------
# Check 1: Row-count anomaly
# ---------------------------------------------------------------------------
def check_row_count_anomaly(engine) -> CheckResult:
    """
    Compare today's raw_shipments row count to the rolling mean/stddev
    of the last WINDOW_SIZE loads recorded in anomaly_check_results.

    Why raw_shipments and not fact_shipments?
    Because fact_shipments is rebuilt every dbt run — its count is always
    current. raw_shipments reflects what the ingestion loader actually wrote,
    which is the real leading indicator of an ingestion problem.
    """
    check_name = "row_count_anomaly"
    table_name = "raw_shipments"

    # Current count
    with engine.connect() as conn:
        current_count = conn.execute(
            text("SELECT COUNT(*) FROM raw_shipments")
        ).scalar()

    # Historical counts from previous anomaly_check_results runs
    history_df = pd.read_sql(
        """
        SELECT observed_value
        FROM   anomaly_check_results
        WHERE  check_name = 'row_count_anomaly'
        ORDER  BY run_at DESC
        LIMIT  %(window)s
        """,
        engine,
        params={"window": WINDOW_SIZE},
    )

    if len(history_df) < 5:
        # Not enough history to compute a meaningful baseline — log and pass
        return CheckResult(
            check_name=check_name,
            table_name=table_name,
            column_name="",
            status="pass",
            severity="info",
            observed_value=float(current_count),
            expected_value=None,
            z_score=None,
            threshold=Z_THRESHOLD_CRIT,
            message=f"Insufficient history ({len(history_df)} runs). Need >= 5. Observed: {current_count:,} rows.",
        )

    mean   = history_df["observed_value"].mean()
    stddev = history_df["observed_value"].std()

    if stddev == 0 or abs(stddev) < 1e-12:
        diff = abs(float(current_count) - mean)
        rel_diff = diff / max(abs(mean), 1e-6)
        z = 0.0 if rel_diff < 1e-3 else 9999.0
    else:
        z = (current_count - mean) / stddev

    status, severity = _classify(z)
    message = (
        f"Row count: {current_count:,} | "
        f"Window mean: {mean:,.0f} | Stddev: {stddev:,.0f} | z={z:.2f}"
    )
    if status != "pass":
        message += f" *** ANOMALY: |z|={abs(z):.2f} > threshold {Z_THRESHOLD_CRIT}"

    logger.info("[%s] %s", check_name, message)
    return CheckResult(
        check_name=check_name,
        table_name=table_name,
        column_name="",
        status=status,
        severity=severity,
        observed_value=float(current_count),
        expected_value=float(mean),
        z_score=float(z),
        threshold=Z_THRESHOLD_CRIT,
        message=message,
    )


# ---------------------------------------------------------------------------
# Check 2: Metric drift — average shipping cost
# ---------------------------------------------------------------------------
def check_metric_drift(
    engine,
    metric_name:  str   = "avg_days_shipping_real",
    sql_metric:   str   = "SELECT AVG(`Days for shipping (real)`) FROM raw_shipments",
    table_name:   str   = "raw_shipments",
    column_name:  str   = "Days for shipping (real)",
) -> CheckResult:
    """
    Compare today's value of a numeric metric (e.g., average shipping days)
    against the trailing WINDOW_SIZE rolling distribution.

    This catches business-level drift: "average delivery time jumped this week"
    won't trigger any static test, but it's exactly what an ops team wants to know.

    The function is parameterised so it can be reused for any scalar metric.
    """
    check_name = metric_name

    # Current value — cast to float: MySQL AVG() returns decimal.Decimal
    with engine.connect() as conn:
        current_val = conn.execute(text(sql_metric)).scalar()
        if current_val is None:
            return CheckResult(
                check_name=check_name,
                table_name=table_name,
                column_name=column_name,
                status="pass",
                severity="info",
                observed_value=None,
                expected_value=None,
                z_score=None,
                threshold=Z_THRESHOLD_CRIT,
                message="Metric returned NULL — table may be empty.",
            )
    current_val = float(current_val)  # MySQL AVG() returns decimal.Decimal

    history_df = pd.read_sql(
        """
        SELECT observed_value
        FROM   anomaly_check_results
        WHERE  check_name = %(check_name)s
        ORDER  BY run_at DESC
        LIMIT  %(window)s
        """,
        engine,
        params={"check_name": check_name, "window": WINDOW_SIZE},
    )

    if len(history_df) < 5:
        return CheckResult(
            check_name=check_name,
            table_name=table_name,
            column_name=column_name,
            status="pass",
            severity="info",
            observed_value=float(current_val),
            expected_value=None,
            z_score=None,
            threshold=Z_THRESHOLD_CRIT,
            message=f"Insufficient history. Observed: {current_val:.4f}",
        )

    mean   = history_df["observed_value"].mean()
    stddev = history_df["observed_value"].std()

    if stddev == 0 or abs(stddev) < 1e-12:
        # Use relative tolerance (0.1%) to distinguish real shifts from
        # MySQL DECIMAL floating-point noise in repeated reads.
        diff = abs(current_val - mean)
        rel_diff = diff / max(abs(mean), 1e-6)
        z = 0.0 if rel_diff < 1e-3 else 9999.0
    else:
        z = (current_val - mean) / stddev

    status, severity = _classify(z)
    message = (
        f"{metric_name}: {current_val:.4f} | "
        f"Window mean: {mean:.4f} | Stddev: {stddev:.4f} | z={z:.2f}"
    )
    if status != "pass":
        message += f" *** ANOMALY: |z|={abs(z):.2f} > threshold {Z_THRESHOLD_CRIT}"

    logger.info("[%s] %s", check_name, message)
    return CheckResult(
        check_name=check_name,
        table_name=table_name,
        column_name=column_name,
        status=status,
        severity=severity,
        observed_value=float(current_val),
        expected_value=float(mean),
        z_score=float(z),
        threshold=Z_THRESHOLD_CRIT,
        message=message,
    )


# ---------------------------------------------------------------------------
# Check 3: Null-rate drift
# ---------------------------------------------------------------------------
def check_null_rate_drift(
    engine,
    table_name:  str  = "raw_shipments",
    column_name: str  = "Customer Email",
) -> CheckResult:
    """
    Track the null percentage of a key column over time.

    Why this matters: a basic `not_null` test fails only if *all* values are
    null. But if an upstream system starts sending 10% null emails instead of
    0.2%, the not_null test still passes while the data is getting worse.
    Null-rate drift catches "upstream started sending incomplete records" early.
    """
    check_name = f"null_rate_drift__{table_name}__{column_name.replace(' ', '_')}"

    sql = f"""
        SELECT
            SUM(CASE WHEN `{column_name}` IS NULL OR `{column_name}` = '' THEN 1 ELSE 0 END)
                / COUNT(*) AS null_rate
        FROM `{table_name}`
    """
    with engine.connect() as conn:
        current_rate = conn.execute(text(sql)).scalar()
        if current_rate is None:
            current_rate = 0.0
        current_rate = float(current_rate)  # decimal.Decimal -> float

    history_df = pd.read_sql(
        """
        SELECT observed_value
        FROM   anomaly_check_results
        WHERE  check_name = %(check_name)s
        ORDER  BY run_at DESC
        LIMIT  %(window)s
        """,
        engine,
        params={"check_name": check_name, "window": WINDOW_SIZE},
    )

    if len(history_df) < 5:
        return CheckResult(
            check_name=check_name,
            table_name=table_name,
            column_name=column_name,
            status="pass",
            severity="info",
            observed_value=float(current_rate),
            expected_value=None,
            z_score=None,
            threshold=Z_THRESHOLD_CRIT,
            message=f"Insufficient history. Null rate: {current_rate:.2%}",
        )

    mean   = history_df["observed_value"].mean()
    stddev = history_df["observed_value"].std()

    if stddev == 0:
        z = 0.0 if current_rate == mean else float("inf")
    else:
        z = (current_rate - mean) / stddev

    status, severity = _classify(z)
    message = (
        f"Null rate for {column_name}: {current_rate:.2%} | "
        f"Window mean: {mean:.2%} | Stddev: {stddev:.4f} | z={z:.2f}"
    )
    if status != "pass":
        message += f" *** ANOMALY: |z|={abs(z):.2f} > threshold {Z_THRESHOLD_CRIT}"

    logger.info("[%s] %s", check_name, message)
    return CheckResult(
        check_name=check_name,
        table_name=table_name,
        column_name=column_name,
        status=status,
        severity=severity,
        observed_value=float(current_rate),
        expected_value=float(mean),
        z_score=float(z),
        threshold=Z_THRESHOLD_CRIT,
        message=message,
    )


# ---------------------------------------------------------------------------
# Persist results
# ---------------------------------------------------------------------------
def persist_check_results(results: list[CheckResult], engine) -> None:
    """Insert a batch of CheckResult objects into anomaly_check_results."""
    if not results:
        return

    insert_sql = text("""
        INSERT INTO anomaly_check_results
            (run_at, check_name, table_name, column_name, status, severity,
             observed_value, expected_value, z_score, threshold, message)
        VALUES
            (:run_at, :check_name, :table_name, :column_name, :status, :severity,
             :observed_value, :expected_value, :z_score, :threshold, :message)
    """)

    import math
    def _safe_float(v):
        """Convert inf/nan to a large finite sentinel MySQL can store."""
        if v is None:
            return None
        if math.isinf(v) or math.isnan(v):
            return 9999.0
        return v

    rows = [
        {
            "run_at":         r.run_at,
            "check_name":     r.check_name,
            "table_name":     r.table_name,
            "column_name":    r.column_name,
            "status":         r.status,
            "severity":       r.severity,
            "observed_value": _safe_float(r.observed_value),
            "expected_value": _safe_float(r.expected_value),
            "z_score":        _safe_float(r.z_score),
            "threshold":      r.threshold,
            "message":        r.message,
        }
        for r in results
    ]

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

    logger.info("Persisted %d anomaly check results.", len(rows))


# ---------------------------------------------------------------------------
# Run all checks
# ---------------------------------------------------------------------------
def run_all_checks() -> list[CheckResult]:
    """
    Execute every registered anomaly check, persist results, and return them.
    Call this from the Airflow task in Phase 4.
    """
    engine = get_engine()
    ensure_observability_tables(engine)

    results = [
        # Check 1: row count
        check_row_count_anomaly(engine),

        # Check 2: metric drift — average days to ship
        check_metric_drift(
            engine,
            metric_name="avg_days_shipping_real",
            sql_metric="SELECT AVG(`Days for shipping (real)`) FROM raw_shipments",
            table_name="raw_shipments",
            column_name="Days for shipping (real)",
        ),

        # Check 2b: metric drift — average profit per order
        check_metric_drift(
            engine,
            metric_name="avg_order_profit",
            sql_metric="SELECT AVG(`Order Profit Per Order`) FROM raw_shipments",
            table_name="raw_shipments",
            column_name="Order Profit Per Order",
        ),

        # Check 3: null rate — customer email
        check_null_rate_drift(
            engine,
            table_name="raw_shipments",
            column_name="Customer Email",
        ),
    ]

    persist_check_results(results, engine)

    # Summary
    statuses = [r.status for r in results]
    criticals = statuses.count("critical")
    warnings  = statuses.count("warn")
    passes    = statuses.count("pass")
    logger.info(
        "Anomaly checks complete — CRITICAL: %d | WARN: %d | PASS: %d",
        criticals, warnings, passes,
    )
    return results


if __name__ == "__main__":
    run_all_checks()
