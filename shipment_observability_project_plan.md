# Shipment Data Quality & Observability Platform
### Full Project Plan

**Goal:** Build a production-style logistics data pipeline where data quality and monitoring are the actual product, not an afterthought — demonstrating the specific skills that separate "built a pipeline" from "can be trusted to run one."

**Target audience for this project:** Data Engineer / Analytics Engineer interviewers, specifically for questions like *"tell me about a time you caught a data quality issue"* and *"how would you monitor a production pipeline."*

**Estimated timeline:** 3–4 weeks at a steady part-time pace (10-15 hrs/week), structured in 6 phases. Each phase ends with a working, demoable increment — never leave the repo in a broken state between sessions.

---

## 0. Before you write any code

### 0.1 Decide and document your scope up front
Write a `PROJECT_GUIDE.md` (you already have this pattern from your last project — reuse it) answering:
- What decision does this pipeline support? (e.g., "ops team needs to know within 1 hour if shipment data ingestion is broken or shipment volumes look abnormal")
- What counts as "broken"? (missing data, stale data, out-of-range data, schema drift — define each concretely for this dataset)
- What's in scope vs explicitly out of scope for v1? (e.g., "no ML anomaly detection in v1, statistical thresholds only — ML is a stretch goal")

Writing this first, before code, is itself something senior engineers do and juniors skip — and it gives you a natural narrative for interviews later.

### 0.2 Pick and inspect the dataset
- Primary recommendation: **DataCo Smart Supply Chain Dataset** (Kaggle) — ~180k rows, shipping/order/delivery fields, known to contain genuine anomalies (negative profits, inconsistent delivery statuses, mixed date formats).
- Before building anything, manually profile it: row counts per column, null rates, distinct value counts on categorical fields, min/max on numeric/date fields. Do this in a throwaway notebook — this profiling *is* how you'll know what checks are worth building later, and you should keep the profiling notebook in the repo under `/notebooks/00_profiling.ipynb` as evidence of process.

### 0.3 Set up the repo skeleton early
```
shipment-observability/
├── ingestion/              # extract + schema drift detection
├── dbt/
│   ├── models/staging/
│   ├── models/intermediate/
│   ├── models/marts/
│   └── tests/               # custom singular tests, anomaly checks
├── observability/
│   ├── test_result_logger.py
│   ├── anomaly_checks.py
│   └── alerting.py
├── orchestration/dags/
├── dashboard/               # Streamlit observability app
├── notebooks/
├── docs/images/
├── docker-compose.yml
├── PROJECT_GUIDE.md
└── README.md
```
Commit this skeleton with empty placeholder files first — gives you a clean, readable commit history from day one (something you flagged as a signal worth caring about).

---

## Phase 1 — Ingestion with schema drift detection (Days 1–3)

**Deliverable:** a Python ingestion script that loads raw CSVs into Postgres, and *fails loudly with a specific message* if the incoming schema doesn't match what's expected.

### Build
1. Define an expected schema per source file (column names, types) in a config file (`ingestion/schemas.py`) — reuse the pattern from your last project, but extend it.
2. On each ingestion run, compare the actual CSV/DataFrame schema to expected:
   - New column appeared → log a warning, don't fail (schema addition is usually safe).
   - Expected column missing → **fail the run**, this is a breaking change.
   - Column type changed (e.g., a numeric column now has text values) → fail the run.
3. Log every drift event (even warnings) to a `schema_drift_log` table in Postgres — timestamp, table, column, drift type. This log is what your dashboard will visualize later.

### Why this matters (know this for interviews)
Schema drift is one of the most common real-world pipeline failures — an upstream API or export changes silently, and a naive pipeline either crashes cryptically or, worse, loads bad data without complaint. Detecting and classifying drift (breaking vs non-breaking) is a genuinely senior-level distinction.

### Acceptance criteria for this phase
- [ ] Running ingestion against the real dataset succeeds and loads cleanly.
- [ ] Deliberately renaming a column in a test CSV causes the ingestion to fail with a specific, readable error naming the missing column.
- [ ] Deliberately changing a numeric column to contain text causes a logged failure, not a silent bad load.
- [ ] `schema_drift_log` table has at least one row after your deliberate-break test.

---

## Phase 2 — dbt modeling layer (Days 4–8)

**Deliverable:** staging → intermediate → marts layers, same pattern as your last project, but for shipment/logistics grain this time.

### Build
1. **Staging models**: one per raw source table — clean types, standardize nulls, rename to consistent conventions (Module 5-style cleaning from earlier).
2. **Intermediate models**: dedup logic where needed, and any pre-aggregation required before hitting the mart grain (avoid fan-out — pre-aggregate the "many" side before joining, exactly like the join module covered).
3. **Marts — star schema**:
   - `fact_shipments` (grain: one row per shipment/order-line) — measures: `days_to_deliver`, `shipping_cost`, `profit`, flags like `is_late`.
   - `dim_customers`, `dim_products`, `dim_warehouses_or_regions`, `dim_dates`.
4. Add standard dbt tests on every model: `not_null` on required fields, `unique` on primary keys, `relationships` for every foreign key.

### Acceptance criteria
- [ ] `dbt run` builds cleanly with zero errors.
- [ ] `dbt test` passes on the clean dataset.
- [ ] Star schema diagram (mermaid ERD, like your last project) is in the README.
- [ ] At least one written paragraph explaining your grain choice for `fact_shipments` and why (this is exactly the kind of design-decision writeup you were missing last time).

---

## Phase 3 — The observability layer (Days 9–15)

This is the phase that makes the project distinct. Don't rush it.

### 3.1 Persist test results over time, not just pass/fail
- Use `dbt-utils` + parse `dbt`'s own `run_results.json`/`elementary-data` package, or write a lightweight custom Python script that runs after `dbt test` and inserts each test's result (test name, table, status, row count of failures, timestamp) into a `dbt_test_results` table.
- This turns dbt tests from a one-time gate into a **time series** — the foundation everything else in this phase builds on.

### 3.2 Static data quality tests (the baseline layer)
Beyond dbt's generic tests, write custom singular tests specific to this domain:
- `assert_delivery_after_shipment_date.sql` — delivery date must never be before ship date.
- `assert_no_negative_shipping_cost.sql`
- `assert_valid_delivery_status.sql` — accepted-values check against a known status list.
- `assert_late_flag_consistency.sql` — `is_late` flag must agree with the actual date math, not just a copied upstream field (a real "business logic vs raw field disagreement" check, the kind we discussed as a hallmark of mature testing).

### 3.3 Statistical anomaly checks (the differentiator layer)
This is the part almost no portfolio project has. Build a small Python module (`observability/anomaly_checks.py`) that runs after each load:
- **Row-count anomaly**: today's load's row count vs a rolling mean/stddev of the last 30 loads — flag if outside ~3 standard deviations.
- **Metric drift**: e.g., average `shipping_cost` or `days_to_deliver` today vs its trailing 30-day distribution — flag significant shifts.
- **Null-rate drift**: percentage of nulls in a key column today vs historical baseline — catches "upstream started sending incomplete records" even when the column technically still passes a basic not-null test on non-null rows.

Store these results in an `anomaly_check_results` table, same shape as your test results table, so both feed the same dashboard.

**Keep the statistics simple and explainable** — z-score against a rolling window is enough. The point is demonstrating you understand *why* this catches things static tests miss, not building a research-grade anomaly detector.

### 3.4 Severity tiers
Classify every check (dbt test or anomaly check) into a severity: `critical` (blocks downstream/halts pipeline), `warning` (logs and alerts but doesn't block), `info` (logged only). Store this as metadata alongside each check definition — this is what lets your orchestration layer make branching decisions in Phase 4.

### Acceptance criteria
- [ ] `dbt_test_results` table has historical rows after 3+ manual pipeline runs.
- [ ] At least 4 custom singular business-logic tests exist and are documented.
- [ ] At least 3 statistical anomaly checks are implemented and demonstrably fire on injected bad data.
- [ ] Every check has an assigned severity tier, stored and queryable.

---

## Phase 4 — Alerting and orchestration (Days 16–19)

**Deliverable:** an Airflow DAG that runs the full pipeline, executes quality/anomaly checks, and takes different actions depending on severity.

### Build
1. **Slack webhook integration** (free — a Slack workspace + incoming webhook URL) — write `observability/alerting.py` with a function that posts a formatted message: which check failed, which table, the actual vs expected values, a timestamp, and severity.
2. **DAG structure**:
   ```
   extract → validate_schema → load_to_warehouse → dbt_run → dbt_test + anomaly_checks
                                                                    │
                                                    ┌───────────────┴───────────────┐
                                                    ▼                                ▼
                                          any critical failure?              only warnings/info?
                                                    │                                │
                                            halt + alert critical            proceed + alert warning
   ```
   Use a `BranchPythonOperator` (or equivalent in your Airflow version) to implement this — critical failures should genuinely stop downstream tasks (e.g., don't let the dashboard refresh from bad data), while warnings should let the pipeline continue but still notify.
3. **Retry and idempotency check**: explicitly verify (and document) that re-running the full DAG after a failure doesn't duplicate data — pick a deliberate strategy (`merge`/upsert or scoped delete-and-reload) and state which one and why in the README, directly answering the gap flagged in your last project's review.

### Acceptance criteria
- [ ] Deliberately injecting a bad row (e.g., delivery date before ship date) into a test load causes a critical Slack alert and halts the DAG before the dashboard-refresh step.
- [ ] Deliberately injecting a smaller anomaly (e.g., a 20% row-count drop) causes a warning alert but the pipeline still completes.
- [ ] Running the full DAG twice in a row produces identical row counts in `fact_shipments` (screenshot or logged proof, referenced in README).

---

## Phase 5 — The observability dashboard (Days 20–24)

**Deliverable:** a Streamlit app, structurally different from your prior KPI dashboard — this one visualizes *pipeline health*, not business metrics.

### Build — suggested pages/sections
1. **Pipeline health overview**: test pass rate over time (line chart), most recently failed checks, current freshness of each table (`time since last successful load`).
2. **Anomaly timeline**: a chart per monitored metric (row count, avg shipping cost, null rate) showing the historical trend with flagged anomaly points highlighted.
3. **Test result drill-down**: a filterable table of all check runs — table, check name, status, severity, timestamp — so someone could audit history the way a real on-call engineer would.
4. **Schema drift log viewer**: a simple table view of the `schema_drift_log` from Phase 1.

### Why this section matters for your narrative
This dashboard is the single artifact that most clearly communicates "I understand observability as a discipline" in an interview — screen-sharing it and walking through a real anomaly you caught is a far stronger answer than describing it verbally.

### Acceptance criteria
- [ ] Dashboard loads real historical data (not just the latest run) — requires you to have actually run the pipeline multiple times before building this.
- [ ] At least one genuinely interesting anomaly is visible in the demo data (inject one deliberately if the real data doesn't produce one naturally).
- [ ] Deployed publicly (Streamlit Community Cloud, free) with a live link in the README, same as your last project.

---

## Phase 6 — Documentation and portfolio polish (Days 25–28)

This phase is not optional — it's where most of the interview value gets unlocked or lost.

### 6.1 README structure
- Problem framing (from your `PROJECT_GUIDE.md`, condensed)
- Architecture diagram (mermaid, like your last project)
- **A dedicated "Data Quality & Observability" section** — this is your differentiator, give it real space: describe the check hierarchy (static vs statistical), severity tiers, and the alerting flow.
- **A short "Incident" writeup**: pick your best injected/caught anomaly, and write it up like a mini postmortem — what broke, how it was detected, what alert fired, what the fix was. This single paragraph is genuinely one of the highest-value pieces of content you can put in a data engineering portfolio.
- Idempotency statement (explicit, one paragraph, addressing exactly what was flagged as missing last time).
- Quickstart (Docker Compose, same pattern as before).
- Tech stack table.
- "What I'd do with more time" section — listing the ML anomaly detection stretch goal and anything else explicitly deferred from 0.1's scope doc. This shows self-awareness about tradeoffs, which reads well to senior interviewers.

### 6.2 Prepare your verbal narrative
Write out (for yourself, doesn't need to go in the repo) 60-second answers to:
- "Walk me through this project."
- "Tell me about a time you caught a data quality issue." → point directly at your incident writeup.
- "How would you extend this for a real production team?" → mention things like PagerDuty integration, dbt Cloud/Elementary's hosted observability, data contracts with upstream teams.

### Acceptance criteria
- [ ] README readable top-to-bottom by someone who has never seen the project, in under 5 minutes, and comes away understanding both what it does and why the quality layer matters.
- [ ] You can verbally explain the project in 60 seconds without looking at notes.

---

## Full timeline summary

| Phase | Focus | Days | Key deliverable |
|---|---|---|---|
| 0 | Scoping + setup | 1 | `PROJECT_GUIDE.md`, repo skeleton, data profiling |
| 1 | Ingestion + schema drift | 2–3 | Drift-detecting loader, `schema_drift_log` |
| 2 | dbt modeling | 4–8 | Star schema, standard dbt tests |
| 3 | Observability layer | 9–15 | Test history, custom + statistical anomaly checks, severity tiers |
| 4 | Orchestration + alerting | 16–19 | Branching Airflow DAG, Slack alerts, idempotency proof |
| 5 | Observability dashboard | 20–24 | Deployed Streamlit health dashboard |
| 6 | Docs + polish | 25–28 | README with incident writeup, rehearsed narrative |

## Tech stack (all free / local-friendly, consistent with your existing setup)

| Layer | Tool |
|---|---|
| Warehouse | PostgreSQL (Docker) |
| Transformation | dbt-core |
| Test history / anomaly logic | Python (pandas, sqlalchemy) |
| Orchestration | Apache Airflow |
| Alerting | Slack incoming webhook |
| Dashboard | Streamlit + Plotly |
| Containerization | Docker Compose |

## Definition of done for the whole project

- [ ] Fresh clone + `docker compose up` runs the full stack with no manual steps beyond documented prerequisites.
- [ ] A deliberately-injected bad row is caught, alerted on, and halts the pipeline appropriately.
- [ ] A deliberately-injected statistical anomaly is caught and visualized on the dashboard.
- [ ] Idempotency is explicitly demonstrated, not just claimed.
- [ ] README contains a real incident writeup, not just architecture description.
- [ ] You can explain every design decision (grain choice, severity tiers, alerting thresholds) out loud without notes.
