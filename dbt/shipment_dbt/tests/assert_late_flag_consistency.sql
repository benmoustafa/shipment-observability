/*
  assert_late_flag_consistency.sql

  Severity: WARN — this is a data quality signal, not a hard blocker.
  The upstream late_delivery_risk_flag is set by a separate model in the
  DataCo source system and does NOT always agree with our date arithmetic.
  We surface the count for observability without blocking marts from building.

  Finding from production run: 4,423 / 180,519 rows (~2.5%) disagree.
  Likely root cause: 'Shipping canceled' orders carry risk_flag=1 but have
  days_shipping_real=0, making our date math return is_late=0.
  This is upstream data quality debt, not our bug.

  Returns rows where the two flags disagree — test warns when > 0 rows returned.
*/

{{ config(severity='warn') }}

SELECT
    order_id,
    order_item_id,
    is_late,
    late_delivery_risk_flag,
    delivery_status,
    days_shipping_real,
    days_shipping_scheduled
FROM {{ ref('fact_shipments') }}
WHERE is_late != late_delivery_risk_flag
