"""
dashboard/app.py

Shipment Data Quality Observability & Executive Analytics Platform.

This platform combines two core capabilities:
  1. Data Pipeline Observability: Schema drift monitoring, dbt test assertions, and z-score anomaly detection.
  2. Executive Supply Chain Analytics: Business intelligence metrics covering revenue, profit margins, regional market performance, shipping mode efficiency, and delivery status distributions.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add project root to sys.path to enable imports
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from observability.db import get_engine

# Page Configuration
st.set_page_config(
    page_title="Shipment Observability & Analytics Platform",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Enterprise CSS Styling (Clean, Dark Theme, No Emojis)
st.markdown("""
    <style>
        .main {
            background-color: #0b0d14;
            color: #e2e8f0;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        div[data-testid="stMetricValue"] {
            font-size: 26px;
            font-weight: 700;
            color: #38bdf8;
        }
        div[data-testid="stMetricLabel"] {
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #94a3b8;
        }
        .stAlert {
            background-color: #1e293b;
            color: #f8fafc;
            border: 1px solid #334155;
            border-radius: 6px;
        }
        h1, h2, h3, h4 {
            color: #ffffff !important;
            font-weight: 600;
        }
        .css-1544g2n {
            background-color: #0f172a;
        }
        /* Custom table styling */
        .dataframe {
            font-size: 13px;
        }
    </style>
""", unsafe_allow_html=True)

IS_DEMO_MODE = False


# ===========================================================================
# Sample Data Generators for Cloud Demo Mode (Database Offline)
# ===========================================================================

def get_demo_dbt_data() -> pd.DataFrame:
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
        ("assert_late_flag_consistency", "fact_shipments", "", "warn", "warning", 4423, "Got 4423 discrepancies (Shipping Canceled carrying risk_flag=1 with 0 shipping days)"),
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
    now = datetime.now()
    records = []
    for i in range(10):
        t = now - pd.Timedelta(hours=i * 2)
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
    now = datetime.now()
    return pd.DataFrame([{
        "detected_at": now - pd.Timedelta(days=2),
        "source_table": "raw_shipments",
        "column_name": "Promotional_Tag",
        "drift_type": "NEW_COLUMN",
        "severity": "NON_BREAKING",
        "detail": "New optional column 'Promotional_Tag' detected in raw CSV stream."
    }])


def get_demo_analytics_data() -> dict[str, pd.DataFrame]:
    """Supply chain metrics snapshots based on the DataCo Smart Supply Chain dataset."""
    markets_df = pd.DataFrame([
        {"Market": "LATAM", "Sales": 12640200, "Profit": 1352100, "Orders": 51594},
        {"Market": "Europe", "Sales": 10820500, "Profit": 1154300, "Orders": 50252},
        {"Market": "Pacific Asia", "Sales": 8510400, "Profit": 921000, "Orders": 41260},
        {"Market": "USCA", "Sales": 3350200, "Profit": 361200, "Orders": 25799},
        {"Market": "Africa", "Sales": 1471300, "Profit": 178700, "Orders": 11614},
    ])

    shipping_modes_df = pd.DataFrame([
        {"Shipping Mode": "Standard Class", "Total Orders": 107752, "Late Rate (%)": 38.2},
        {"Shipping Mode": "Second Class", "Total Orders": 35216, "Late Rate (%)": 76.5},
        {"Shipping Mode": "First Class", "Total Orders": 27814, "Late Rate (%)": 95.3},
        {"Shipping Mode": "Same Day", "Total Orders": 9737, "Late Rate (%)": 46.1},
    ])

    status_df = pd.DataFrame([
        {"Order Status": "COMPLETE", "Volume": 59530},
        {"Order Status": "PENDING_PAYMENT", "Volume": 39832},
        {"Order Status": "PROCESSING", "Volume": 21902},
        {"Order Status": "PENDING", "Volume": 20227},
        {"Order Status": "CLOSED", "Volume": 19616},
        {"Order Status": "ON_HOLD", "Volume": 9804},
        {"Order Status": "SUSPECTED_FRAUD", "Volume": 4062},
        {"Order Status": "CANCELED", "Volume": 3692},
        {"Order Status": "PAYMENT_REVIEW", "Volume": 1854},
    ])

    categories_df = pd.DataFrame([
        {"Category": "Cleats", "Sales": 4431000, "Profit Margin (%)": 11.2},
        {"Category": "Men's Footwear", "Sales": 3892000, "Profit Margin (%)": 10.8},
        {"Category": "Women's Apparel", "Sales": 3105000, "Profit Margin (%)": 12.1},
        {"Category": "Water Sports", "Sales": 3042000, "Profit Margin (%)": 10.5},
        {"Category": "Camping & Hiking", "Sales": 2980000, "Profit Margin (%)": 11.4},
        {"Category": "Cardio Equipment", "Sales": 2450000, "Profit Margin (%)": 9.8},
    ])

    return {
        "markets": markets_df,
        "shipping_modes": shipping_modes_df,
        "statuses": status_df,
        "categories": categories_df,
    }


# ===========================================================================
# Data Loading Handler (Database query vs Fallback)
# ===========================================================================

def load_all_data():
    global IS_DEMO_MODE
    try:
        engine = get_engine()
        with engine.connect() as conn:
            pass

        df_dbt = pd.read_sql("SELECT run_id, run_at, test_name, model_name, column_name, status, severity, failure_count, message FROM dbt_test_results ORDER BY run_at DESC", engine)
        df_anom = pd.read_sql("SELECT run_at, check_name, table_name, column_name, status, severity, observed_value, expected_value, z_score, message FROM anomaly_check_results ORDER BY run_at DESC", engine)
        try:
            df_drift = pd.read_sql("SELECT detected_at, source_table, column_name, drift_type, severity, detail FROM schema_drift_log ORDER BY detected_at DESC", engine)
        except Exception:
            df_drift = pd.DataFrame(columns=["detected_at", "source_table", "column_name", "drift_type", "severity", "detail"])

        IS_DEMO_MODE = False
        return df_dbt, df_anom, df_drift, None
    except Exception:
        IS_DEMO_MODE = True
        return get_demo_dbt_data(), get_demo_anomaly_data(), get_demo_drift_data(), get_demo_analytics_data()


df_dbt, df_anom, df_drift, analytics_data = load_all_data()

# ===========================================================================
# Sidebar & Layout Navigation
# ===========================================================================

st.sidebar.markdown("### Navigation")
page = st.sidebar.radio(
    "Select Module",
    [
        "Pipeline Health Overview",
        "Supply Chain Analytics",
        "Statistical Anomalies",
        "dbt Test Audit History",
        "Schema Drift Log",
    ]
)

st.markdown("<h1 style='text-align: left;'>Shipment Data Observability & Analytics</h1>", unsafe_allow_html=True)
st.markdown("<p style='color: #64748b;'>Enterprise data quality monitoring, drift control, and logistics performance analytics.</p>", unsafe_allow_html=True)
st.markdown("---")

if IS_DEMO_MODE:
    st.info("[DEMO MODE] Remote database host offline. Displaying static snapshot analytics and pipeline observability metrics.")

# ===========================================================================
# 1. Pipeline Health Overview Page
# ===========================================================================
if page == "Pipeline Health Overview":
    st.subheader("System Health Summary")

    total_runs = df_dbt["run_id"].nunique() if not df_dbt.empty else 0

    if not df_dbt.empty:
        latest_run_id = df_dbt["run_id"].iloc[0]
        latest_run_data = df_dbt[df_dbt["run_id"] == latest_run_id]
        passed_tests = len(latest_run_data[latest_run_data["status"] == "pass"])
        total_tests = len(latest_run_data)
        pass_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 100
    else:
        pass_rate = 100.0
        latest_run_data = pd.DataFrame()

    anom_failures = len(df_anom[(df_anom["status"] != "pass")]) if not df_anom.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Pipeline Status", "HEALTHY" if pass_rate >= 95 else "ATTENTION")
    with col2:
        st.metric("Test Pass Rate", f"{pass_rate:.1f}%")
    with col3:
        st.metric("Total Executed Runs", f"{total_runs}")
    with col4:
        st.metric("Active Anomaly Alerts", f"{anom_failures}")

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Open System Alerts & Quality Discrepancies")

    alerts = []
    if not latest_run_data.empty:
        failures = latest_run_data[latest_run_data["status"] != "pass"]
        for _, row in failures.iterrows():
            alerts.append({
                "Type": "Static Business Rule",
                "Check Name": row["test_name"],
                "Model Target": row["model_name"],
                "Severity": row["severity"],
                "Message": row["message"]
            })

    if not df_anom.empty:
        latest_anoms = df_anom[df_anom["run_at"] == df_anom["run_at"].max()]
        failures_anom = latest_anoms[latest_anoms["status"] != "pass"]
        for _, row in failures_anom.iterrows():
            alerts.append({
                "Type": "Statistical Anomaly",
                "Check Name": row["check_name"],
                "Model Target": row["table_name"],
                "Severity": row["severity"],
                "Message": row["message"]
            })

    if alerts:
        alerts_df = pd.DataFrame(alerts)
        st.dataframe(
            alerts_df.style.map(
                lambda val: "color: #ef4444; font-weight: bold;" if val == "critical" else (
                    "color: #f59e0b; font-weight: bold;" if val == "warning" else ""
                ),
                subset=["Severity"]
            ),
            use_container_width=True
        )
    else:
        st.success("All quality assertions and statistical anomaly checks passed cleanly.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Historical Test Pass Rates")
    if not df_dbt.empty:
        trend_df = df_dbt.groupby(["run_id", "run_at"]).apply(
            lambda x: pd.Series({
                "Pass Rate (%)": (len(x[x["status"] == "pass"]) / len(x)) * 100,
                "Total Assertions": len(x)
            }),
            include_groups=False
        ).reset_index().sort_values("run_at")

        fig = px.line(
            trend_df, x="run_at", y="Pass Rate (%)",
            title="Static Assertion Reliability Trend",
            labels={"run_at": "Execution Timestamp", "Pass Rate (%)": "Pass Rate"},
            template="plotly_dark",
            markers=True
        )
        fig.update_traces(line=dict(color="#38bdf8", width=3))
        st.plotly_chart(fig, use_container_width=True)

# ===========================================================================
# 2. Executive Supply Chain Analytics Page (NEW)
# ===========================================================================
elif page == "Supply Chain Analytics":
    st.subheader("Executive Supply Chain & Financial Performance")

    if analytics_data is None:
        # DB is connected — query actual tables directly
        engine = get_engine()
        markets_df = pd.read_sql("SELECT market AS Market, SUM(sales) AS Sales, SUM(order_profit) AS Profit, COUNT(*) AS Orders FROM fact_shipments GROUP BY market ORDER BY Sales DESC", engine)
        shipping_modes_df = pd.read_sql("SELECT shipping_mode AS `Shipping Mode`, COUNT(*) AS `Total Orders`, AVG(is_late)*100 AS `Late Rate (%)` FROM fact_shipments GROUP BY shipping_mode", engine)
        status_df = pd.read_sql("SELECT order_status AS `Order Status`, COUNT(*) AS Volume FROM fact_shipments GROUP BY order_status ORDER BY Volume DESC", engine)
        categories_df = pd.read_sql("SELECT category_name AS Category, SUM(sales) AS Sales, (SUM(order_profit)/SUM(sales))*100 AS `Profit Margin (%)` FROM fact_shipments GROUP BY category_name ORDER BY Sales DESC LIMIT 6", engine)
    else:
        markets_df = analytics_data["markets"]
        shipping_modes_df = analytics_data["shipping_modes"]
        status_df = analytics_data["statuses"]
        categories_df = analytics_data["categories"]

    total_sales = markets_df["Sales"].sum()
    total_profit = markets_df["Profit"].sum()
    total_orders = markets_df["Orders"].sum()
    avg_late_rate = (shipping_modes_df["Late Rate (%)"] * shipping_modes_df["Total Orders"]).sum() / shipping_modes_df["Total Orders"].sum()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Revenue", f"${total_sales:,.0f}")
    with col2:
        st.metric("Net Order Profit", f"${total_profit:,.0f}")
    with col3:
        st.metric("Total Shipment Volume", f"{total_orders:,}")
    with col4:
        st.metric("Overall Late Delivery Rate", f"{avg_late_rate:.1f}%")

    st.markdown("<br>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)

    with c1:
        fig_mkt = px.bar(
            markets_df, x="Market", y=["Sales", "Profit"],
            barmode="group",
            title="Revenue & Net Profit by Regional Market",
            template="plotly_dark",
            color_discrete_sequence=["#38bdf8", "#34d399"]
        )
        fig_mkt.update_layout(yaxis_title="Amount ($)")
        st.plotly_chart(fig_mkt, use_container_width=True)

    with c2:
        fig_late = px.bar(
            shipping_modes_df, x="Shipping Mode", y="Late Rate (%)",
            title="Late Delivery Rate by Shipping Class",
            template="plotly_dark",
            color="Late Rate (%)",
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig_late, use_container_width=True)

    c3, c4 = st.columns(2)

    with c3:
        fig_status = px.pie(
            status_df, names="Order Status", values="Volume",
            title="Order Status Distribution",
            template="plotly_dark",
            hole=0.4
        )
        st.plotly_chart(fig_status, use_container_width=True)

    with c4:
        fig_cat = px.bar(
            categories_df, y="Category", x="Sales", orientation="h",
            title="Top Performing Categories by Sales Volume",
            template="plotly_dark",
            color_discrete_sequence=["#818cf8"]
        )
        fig_cat.update_layout(xaxis_title="Sales ($)")
        st.plotly_chart(fig_cat, use_container_width=True)

# ===========================================================================
# 3. Statistical Anomalies Page
# ===========================================================================
elif page == "Statistical Anomalies":
    st.subheader("Statistical Anomaly Timelines")
    st.markdown("Metrics evaluated against a rolling 30-run baseline using z-score statistical variance.")

    if not df_anom.empty:
        unique_checks = df_anom["check_name"].unique()
        selected_check = st.selectbox("Select Monitored Metric:", unique_checks)

        check_data = df_anom[df_anom["check_name"] == selected_check].sort_values("run_at")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=check_data["run_at"], y=check_data["observed_value"],
            mode="lines+markers",
            name="Observed Value",
            line=dict(color="#38bdf8", width=2),
            marker=dict(size=6)
        ))
        fig.add_trace(go.Scatter(
            x=check_data["run_at"], y=check_data["expected_value"],
            mode="lines",
            name="Expected (Rolling Mean)",
            line=dict(color="#64748b", dash="dash")
        ))

        anoms = check_data[check_data["status"] != "pass"]
        if not anoms.empty:
            fig.add_trace(go.Scatter(
                x=anoms["run_at"], y=anoms["observed_value"],
                mode="markers",
                name="Flagged Anomaly",
                marker=dict(color="#ef4444", size=10, symbol="x")
            ))

        fig.update_layout(
            title=f"Statistical Variance Evaluation: {selected_check}",
            xaxis_title="Run Timestamp",
            yaxis_title="Observed Metric",
            template="plotly_dark",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Metric Execution Logs")
        st.dataframe(check_data[["run_at", "observed_value", "expected_value", "z_score", "status", "message"]], use_container_width=True)
    else:
        st.info("No anomaly logs found in anomaly_check_results.")

# ===========================================================================
# 4. dbt Test Audit Page
# ===========================================================================
elif page == "dbt Test Audit History":
    st.subheader("dbt Test Execution Audit History")
    st.markdown("Comprehensive log of all structural, referential integrity, and custom business logic assertions.")

    if not df_dbt.empty:
        cols = st.columns(3)
        with cols[0]:
            search_query = st.text_input("Filter by Assertion Name:")
        with cols[1]:
            filter_status = st.multiselect("Filter by Status:", df_dbt["status"].unique())
        with cols[2]:
            filter_severity = st.multiselect("Filter by Severity:", df_dbt["severity"].unique())

        filtered_df = df_dbt
        if search_query:
            filtered_df = filtered_df[filtered_df["test_name"].str.contains(search_query, case=False)]
        if filter_status:
            filtered_df = filtered_df[filtered_df["status"].isin(filter_status)]
        if filter_severity:
            filtered_df = filtered_df[filtered_df["severity"].isin(filter_severity)]

        st.dataframe(filtered_df, use_container_width=True)
    else:
        st.info("No assertion records logged.")

# ===========================================================================
# 5. Schema Drift Log Page
# ===========================================================================
elif page == "Schema Drift Log":
    st.subheader("Upstream Schema Drift Log")
    st.markdown("Pre-ingest structural drift events caught before loading into raw warehouse tables.")

    if not df_drift.empty:
        st.dataframe(df_drift, use_container_width=True)
    else:
        st.success("Zero schema drift events logged. Source files match predefined YAML data contracts.")
