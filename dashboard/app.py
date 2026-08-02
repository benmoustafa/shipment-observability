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
@st.cache_resource
def get_db_connection():
    return get_engine()


engine = get_db_connection()


# Helper: Load tables
def load_dbt_results():
    query = """
        SELECT run_id, run_at, test_name, model_name, column_name, status, severity, failure_count, message
        FROM dbt_test_results
        ORDER BY run_at DESC
    """
    return pd.read_sql(query, engine)


def load_anomaly_results():
    query = """
        SELECT run_at, check_name, table_name, column_name, status, severity, observed_value, expected_value, z_score, message
        FROM anomaly_check_results
        ORDER BY run_at DESC
    """
    return pd.read_sql(query, engine)


def load_schema_drift():
    query = """
        SELECT detected_at, source_table, column_name, drift_type, severity, detail
        FROM schema_drift_log
        ORDER BY detected_at DESC
    """
    try:
        return pd.read_sql(query, engine)
    except Exception:
        # Returns empty if table does not exist
        return pd.DataFrame(columns=["detected_at", "source_table", "column_name", "drift_type", "severity", "detail"])


# Load Data
df_dbt = load_dbt_results()
df_anom = load_anomaly_results()
df_drift = load_schema_drift()

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
