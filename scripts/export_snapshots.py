"""
scripts/export_snapshots.py

Exports aggregated snapshot datasets from MySQL to data/snapshots/*.csv.
These lightweight CSV files are committed to Git by the pipeline runner to power
the live Streamlit Cloud dashboard automatically without requiring cloud database connectivity.
"""

from pathlib import Path
import pandas as pd
from observability.db import get_engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_DIR = PROJECT_ROOT / "data" / "snapshots"


def export_snapshots():
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    engine = get_engine()

    print(f"Exporting snapshots to {SNAPSHOT_DIR}...")

    # 1. Supply Chain Analytics - Revenue & Profit by Market
    df_market = pd.read_sql(
        """
        SELECT 
            market,
            ROUND(SUM(sales), 2) AS total_revenue,
            ROUND(SUM(order_profit), 2) AS net_profit,
            COUNT(*) AS order_count
        FROM shipment_observability_marts.fact_shipments
        GROUP BY market
        ORDER BY total_revenue DESC
        """,
        engine,
    )
    df_market.to_csv(SNAPSHOT_DIR / "analytics_revenue_by_market.csv", index=False)

    # 2. Shipping Mode Performance
    df_shipping = pd.read_sql(
        """
        SELECT 
            shipping_mode,
            COUNT(*) AS total_shipments,
            SUM(CASE delivery_status WHEN 'Late delivery' THEN 1 ELSE 0 END) AS late_shipments,
            ROUND(AVG(days_shipping_real), 2) AS avg_real_days,
            ROUND(AVG(days_shipping_scheduled), 2) AS avg_scheduled_days,
            ROUND(SUM(CASE delivery_status WHEN 'Late delivery' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_rate
        FROM shipment_observability_marts.fact_shipments
        GROUP BY shipping_mode
        """,
        engine,
    )
    df_shipping.to_csv(SNAPSHOT_DIR / "analytics_late_rate_by_shipping.csv", index=False)

    # 3. Order Status Breakdown
    df_status = pd.read_sql(
        """
        SELECT 
            order_status,
            COUNT(*) AS total_orders
        FROM shipment_observability_marts.fact_shipments
        GROUP BY order_status
        ORDER BY total_orders DESC
        """,
        engine,
    )
    df_status.to_csv(SNAPSHOT_DIR / "analytics_order_status.csv", index=False)

    # 4. Top Category Performance
    df_cat = pd.read_sql(
        """
        SELECT 
            p.category_name,
            ROUND(SUM(f.sales), 2) AS total_sales,
            ROUND(SUM(f.order_profit), 2) AS net_profit
        FROM shipment_observability_marts.fact_shipments f
        JOIN shipment_observability_marts.dim_products p ON f.product_id = p.product_id
        GROUP BY p.category_name
        ORDER BY total_sales DESC
        LIMIT 10
        """,
        engine,
    )
    df_cat.to_csv(SNAPSHOT_DIR / "analytics_category_performance.csv", index=False)

    # 5. Pipeline Summary Metrics
    df_kpi = pd.read_sql(
        """
        SELECT 
            (SELECT COUNT(*) FROM shipment_observability_marts.fact_shipments) AS total_shipments,
            (SELECT ROUND(SUM(sales), 2) FROM shipment_observability_marts.fact_shipments) AS total_revenue,
            (SELECT ROUND(SUM(order_profit), 2) FROM shipment_observability_marts.fact_shipments) AS total_profit,
            (SELECT ROUND(SUM(CASE delivery_status WHEN 'Late delivery' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) FROM shipment_observability_marts.fact_shipments) AS late_rate
        """,
        engine,
    )
    df_kpi.to_csv(SNAPSHOT_DIR / "analytics_kpi_summary.csv", index=False)

    # 6. Observability: dbt Test Audit History
    df_dbt = pd.read_sql(
        """
        SELECT run_at, test_name, status, severity, execution_time_s, message
        FROM dbt_test_results
        ORDER BY run_at DESC
        """,
        engine,
    )
    df_dbt.to_csv(SNAPSHOT_DIR / "dbt_test_audit.csv", index=False)

    # 7. Observability: Anomaly Checks Audit
    df_anom = pd.read_sql(
        """
        SELECT run_at, check_name, status, severity, observed_value, expected_value, z_score, message
        FROM anomaly_check_results
        ORDER BY run_at DESC
        """,
        engine,
    )
    df_anom.to_csv(SNAPSHOT_DIR / "anomaly_audit.csv", index=False)

    # 8. Observability: Schema Drift Log
    try:
        df_drift = pd.read_sql(
            """
            SELECT logged_at, source_name, drift_type, severity, column_name, detail
            FROM schema_drift_log
            ORDER BY logged_at DESC
            """,
            engine,
        )
        df_drift.to_csv(SNAPSHOT_DIR / "schema_drift_audit.csv", index=False)
    except Exception:
        pd.DataFrame(columns=["logged_at", "source_name", "drift_type", "severity", "column_name", "detail"]).to_csv(
            SNAPSHOT_DIR / "schema_drift_audit.csv", index=False
        )

    print("Snapshot export complete.")


if __name__ == "__main__":
    export_snapshots()
