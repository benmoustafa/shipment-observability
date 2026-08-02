"""
dashboard/app.py

Streamlit Data Quality & Pipeline Observability Dashboard.

Unlike standard KPI dashboards that show sales or profit, this dashboard
focuses entirely on operational health:
  1. Pipeline Health Overview: Run status, freshness, and aggregate test pass rates.
  2. Anomaly Timelines: Charts showing rolling metrics (row counts, null rates, numeric averages)
     with anomalies clearly marked.
  3. Test Run History: Searchable and filterable audit trails of all dbt test executions.
  4. Schema Drift Logs: Visualization of upstream structural modifications.
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import sys

# Add project root to sys.path to enable imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from observability.db import get_engine

# Page Config
st.set_page_config(
    page_title="Shipment Ingestion & Quality Observability",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium Theme styling overrides (Custom CSS)
st.markdown("""
    <style>
        .main {
            background-color: #0f111a;
            color: #eceff1;
        }
        div[data-testid="stMetricValue"] {
            font-size: 28px;
            font-weight: 700;
            color: #00e676;
        }
        div[data-testid="stMetricDelta"] {
            font-size: 14px;
        }
        .stAlert {
            background-color: #1a1c23;
            color: #eceff1;
            border: 1px solid #37474f;
        }
        /* Style headers */
        h1, h2, h3 {
            color: #ffffff !important;
            font-family: 'Outfit', 'Inter', sans-serif;
        }
    </style>
""", unsafe_allow_html=True)


# Database Engine
# Database Connection & Fallback Data Handlers
IS_DEMO_MODE = False

def get_demo_dbt_data() -> pd.DataFrame:
    """Generate realistic dbt test results history for Cloud Demo Mode."""
    now = datetime.now()
    records = []
    runs = [
        ("run_005", now),
        ("run_004", now - pd.Timedelta(hours=6)),
        ("run_003", now - pd.Timedelta(hours=12)),
        ("run_002", now - pd.Timedelta(hours=18)),
        ("run_001", now - pd.Timedelta(hours=24)),
    ]
    
    test_specs = [
        ("not_null_fact_shipments_order_id", "fact_shipments", "order_id", "pass", "info", 0, ""),
        ("not_null_fact_shipments_customer_id", "fact_shipments", "customer_id", "pass", "info", 0, ""),
        ("not_null_fact_shipments_sales", "fact_shipments", "sales", "pass", "info", 0, ""),
        ("accepted_values_fact_shipments_is_late", "fact_shipments", "is_late", "pass", "info", 0, ""),
        ("relationships_fact_shipments_customer_id", "fact_shipments", "customer_id", "pass", "info", 0, ""),
        ("relationships_fact_shipments_product_id", "fact_shipments", "product_id", "pass", "info", 0, ""),
        ("unique_dim_customers_customer_id", "dim_customers", "customer_id", "pass", "info", 0, ""),
        ("unique_dim_products_product_id", "dim_products", "product_id", "pass", "info", 0, ""),
        ("unique_dim_dates_date_id", "dim_dates", "date_id", "pass", "info", 0, ""),
        ("assert_ship_date_after_order_date", "fact_shipments", "", "pass", "info", 0, ""),
        ("assert_no_negative_shipping_quantity", "fact_shipments", "order_item_quantity", "pass", "info", 0, ""),
        ("assert_valid_delivery_status", "fact_shipments", "delivery_status", "pass", "info", 0, ""),
        ("assert_late_flag_consistency", "fact_shipments", "", "warn", "warning", 4423, "Got 4423 results, configured to warn if != 0"),
    ]
    
    for run_id, run_at in runs:
        for t_name, model, col, status, severity, fail_cnt, msg in test_specs:
            records.append({
                "run_id": run_id,
                "run_at": run_at,
                "test_name": t_name,
                "model_name": model,
                "column_name": col,
                "status": status,
                "severity": severity,
                "failure_count": fail_cnt,
                "message": msg
            })
    return pd.DataFrame(records)


def get_demo_anomaly_data() -> pd.DataFrame:
    """Generate realistic anomaly check history for Cloud Demo Mode."""
    now = datetime.now()
    records = []
    
    for i in range(10):
        t = now - pd.Timedelta(hours=i*2)
        records.append({
            "run_at": t, "check_name": "row_count_anomaly", "table_name": "raw_shipments", "column_name": "",
            "status": "pass", "severity": "info", "observed_value": 180519.0, "expected_value": 180519.0, "z_score": 0.0,
            "message": "Row count: 180,519 | Window mean: 180,519 | Stddev: 0 | z=0.00"
        })
        records.append({
            "run_at": t, "check_name": "avg_days_shipping_real", "table_name": "raw_shipments", "column_name": "Days for shipping (real)",
            "status": "pass", "severity": "info", "observed_value": 3.4977, "expected_value": 3.4977, "z_score": 0.0,
            "message": "avg_days_shipping_real: 3.4977 | Window mean: 3.4977 | Stddev: 0.0000 | z=0.00"
        })
        records.append({
            "run_at": t, "check_name": "avg_order_profit", "table_name": "raw_shipments", "column_name": "Order Profit Per Order",
            "status": "pass", "severity": "info", "observed_value": 21.9750, "expected_value": 21.9750, "z_score": 0.0,
            "message": "avg_order_profit: 21.9750 | Window mean: 21.9750 | Stddev: 0.0000 | z=0.00"
        })
        records.append({
            "run_at": t, "check_name": "null_rate_drift__raw_shipments__Customer_Email", "table_name": "raw_shipments", "column_name": "Customer Email",
            "status": "pass", "severity": "info", "observed_value": 0.0, "expected_value": 0.0, "z_score": 0.0,
            "message": "Null rate for Customer Email: 0.00% | Window mean: 0.00% | Stddev: 0.0000 | z=0.00"
        })
        
    return pd.DataFrame(records)


def get_demo_drift_data() -> pd.DataFrame:
    """Generate realistic schema drift history for Cloud Demo Mode."""
    now = datetime.now()
    return pd.DataFrame([{
        "detected_at": now - pd.Timedelta(days=2),
        "source_table": "raw_shipments",
        "column_name": "Promotional_Tag",
        "drift_type": "NEW_COLUMN",
        "severity": "NON_BREAKING",
        "detail": "New optional column 'Promotional_Tag' detected in raw CSV stream."
    }])


def load_all_data():
    global IS_DEMO_MODE
    try:
        engine = get_engine()
        # Test connection quickly
        with engine.connect() as conn:
            pass
        
        df_dbt = pd.read_sql("SELECT run_id, run_at, test_name, model_name, column_name, status, severity, failure_count, message FROM dbt_test_results ORDER BY run_at DESC", engine)
        df_anom = pd.read_sql("SELECT run_at, check_name, table_name, column_name, status, severity, observed_value, expected_value, z_score, message FROM anomaly_check_results ORDER BY run_at DESC", engine)
        try:
            df_drift = pd.read_sql("SELECT detected_at, source_table, column_name, drift_type, severity, detail FROM schema_drift_log ORDER BY detected_at DESC", engine)
        except Exception:
            df_drift = pd.DataFrame(columns=["detected_at", "source_table", "column_name", "drift_type", "severity", "detail"])
            
        IS_DEMO_MODE = False
        return df_dbt, df_anom, df_drift
    except Exception as exc:
        IS_DEMO_MODE = True
        return get_demo_dbt_data(), get_demo_anomaly_data(), get_demo_drift_data()


# Load Data with fallback
df_dbt, df_anom, df_drift = load_all_data()

# Sidebar Navigation
st.sidebar.title("🔍 Observability Hub")
st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Navigation Pages",
    ["Dashboard Overview", "Statistical Anomalies", "dbt Test Runs", "Schema Drift Log"]
)

# Header Section
st.markdown("<h1 style='text-align: center;'>📦 Shipment Quality Observability</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #808080;'>Real-time pipeline health, static checks, and statistical anomaly detection.</p>", unsafe_allow_html=True)
st.markdown("---")

if IS_DEMO_MODE:
    st.info("ℹ️ **Cloud Demo Mode**: Local database connection is offline. Showing live interactive snapshot metrics.")

# ===========================================================================
# 1. Overview Page
# ===========================================================================
if page == "Dashboard Overview":
    st.header("Pipeline Health Summary")

    # Metrics Calculations
    total_runs = df_dbt["run_id"].nunique() if not df_dbt.empty else 0
    
    if not df_dbt.empty:
        latest_run_id = df_dbt["run_id"].iloc[0]
        latest_run_data = df_dbt[df_dbt["run_id"] == latest_run_id]
        
        # Static check success rate on latest run
        passed_tests = len(latest_run_data[latest_run_data["status"] == "pass"])
        total_tests = len(latest_run_data)
        pass_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 100
        latest_run_at = latest_run_data["run_at"].iloc[0]
    else:
        pass_rate = 100.0
        latest_run_at = "No runs logged"
        latest_run_data = pd.DataFrame()

    # Anomaly counts in the last 24 hours
    if not df_anom.empty:
        anom_failures = len(df_anom[(df_anom["status"] != "pass")])
    else:
        anom_failures = 0

    # Layout Column metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Last Run Status", "PASS" if pass_rate >= 95 else "WARN", delta=None)
    with col2:
        st.metric("dbt Test Pass Rate", f"{pass_rate:.1f}%", delta=None)
    with col3:
        st.metric("Total Observed Runs", f"{total_runs}", delta=None)
    with col4:
        st.metric("Active Anomalies", f"{anom_failures}", delta=f"{anom_failures} detected", delta_color="inverse")

    # Recent Alerts & Warnings Section
    st.subheader("⚠️ Open Alerts (Non-Passing Checks)")
    
    alerts = []
    if not latest_run_data.empty:
        failures = latest_run_data[latest_run_data["status"] != "pass"]
        for _, row in failures.iterrows():
            alerts.append({
                "Type": "Static Test",
                "Check Name": row["test_name"],
                "Model / Table": row["model_name"],
                "Severity": row["severity"],
                "Message": row["message"]
            })
            
    if not df_anom.empty:
        latest_anoms = df_anom[df_anom["run_at"] == df_anom["run_at"].max()]
        failures_anom = latest_anoms[latest_anoms["status"] != "pass"]
        for _, row in failures_anom.iterrows():
            alerts.append({
                "Type": "Statistical",
                "Check Name": row["check_name"],
                "Model / Table": row["table_name"],
                "Severity": row["severity"],
                "Message": row["message"]
            })

    if alerts:
        alerts_df = pd.DataFrame(alerts)
        st.dataframe(
            alerts_df.style.map(
                lambda val: "color: #ff5252; font-weight: bold;" if val == "critical" else (
                    "color: #ffb74d; font-weight: bold;" if val == "warning" else ""
                ),
                subset=["Severity"]
            ),
            use_container_width=True
        )
    else:
        st.success("All checks are passing cleanly. Pipeline healthy.")

    # Historical Test Trends Chart
    st.subheader("Historical Test Pass Rates")
    if not df_dbt.empty:
        trend_df = df_dbt.groupby(["run_id", "run_at"]).apply(
            lambda x: pd.Series({
                "Pass Rate (%)": (len(x[x["status"] == "pass"]) / len(x)) * 100,
                "Total Tests": len(x)
            }),
            include_groups=False
        ).reset_index().sort_values("run_at")
        
        fig = px.line(
            trend_df, x="run_at", y="Pass Rate (%)",
            title="dbt Test Success Trend",
            labels={"run_at": "Run Timestamp", "Pass Rate (%)": "Pass Rate"},
            template="plotly_dark",
            markers=True
        )
        fig.update_traces(line=dict(color="#00e676", width=3))
        st.plotly_chart(fig, use_container_width=True)

# ===========================================================================
# 2. Statistical Anomalies Page
# ===========================================================================
elif page == "Statistical Anomalies":
    st.header("Statistical Anomaly Timelines")
    st.markdown("We monitor statistical metrics against a rolling 30-run baseline using z-score threshold values.")

    if not df_anom.empty:
        unique_checks = df_anom["check_name"].unique()
        selected_check = st.selectbox("Select metric to monitor:", unique_checks)

        check_data = df_anom[df_anom["check_name"] == selected_check].sort_values("run_at")

        # Metric Trend Chart
        fig = go.Figure()
        
        # Observed Values
        fig.add_trace(go.Scatter(
            x=check_data["run_at"], y=check_data["observed_value"],
            mode="lines+markers",
            name="Observed Value",
            line=dict(color="#00e5ff", width=2),
            marker=dict(size=6)
        ))
        
        # Expected / Rolling Mean
        fig.add_trace(go.Scatter(
            x=check_data["run_at"], y=check_data["expected_value"],
            mode="lines",
            name="Expected (Rolling Mean)",
            line=dict(color="#808080", dash="dash")
        ))
        
        # Anomalies Highlighting
        anoms = check_data[check_data["status"] != "pass"]
        if not anoms.empty:
            fig.add_trace(go.Scatter(
                x=anoms["run_at"], y=anoms["observed_value"],
                mode="markers",
                name="Flagged Anomaly",
                marker=dict(color="#ff1744", size=10, symbol="x")
            ))
            
        fig.update_layout(
            title=f"Statistical Trend & Anomaly Plot: {selected_check}",
            xaxis_title="Run Timestamp",
            yaxis_title="Value",
            template="plotly_dark",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Check Run Execution History")
        st.dataframe(check_data[["run_at", "observed_value", "expected_value", "z_score", "status", "message"]], use_container_width=True)
    else:
        st.info("No anomaly logs found in `anomaly_check_results` yet.")

# ===========================================================================
# 3. dbt Test Page
# ===========================================================================
elif page == "dbt Test Runs":
    st.header("dbt Test History Audit Trail")
    st.markdown("Details of all static assertions executed across pipeline runs.")

    if not df_dbt.empty:
        # Search & Filter controls
        cols = st.columns(3)
        with cols[0]:
            search_query = st.text_input("Search test names:")
        with cols[1]:
            filter_status = st.multiselect("Filter Status:", df_dbt["status"].unique())
        with cols[2]:
            filter_severity = st.multiselect("Filter Severity:", df_dbt["severity"].unique())

        filtered_df = df_dbt
        if search_query:
            filtered_df = filtered_df[filtered_df["test_name"].str.contains(search_query, case=False)]
        if filter_status:
            filtered_df = filtered_df[filtered_df["status"].isin(filter_status)]
        if filter_severity:
            filtered_df = filtered_df[filtered_df["severity"].isin(filter_severity)]

        st.dataframe(filtered_df, use_container_width=True)
    else:
        st.info("No dbt test results found in `dbt_test_results` yet.")

# ===========================================================================
# 4. Schema Drift Page
# ===========================================================================
elif page == "Schema Drift Log":
    st.header("Upstream Schema Drift Logs")
    st.markdown("Chronological list of schema drift events caught before loading data into the warehouse.")

    if not df_drift.empty:
        st.dataframe(df_drift, use_container_width=True)
    else:
        st.success("No schema drift events logged. Warehouse schemas match upstream data contracts.")
