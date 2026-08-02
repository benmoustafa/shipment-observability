{{
    config(
        materialized='table',
        tags=['marts', 'dim']
    )
}}

/*
  dim_customers.sql
  Grain: one row per unique customer_id.

  We take the most recent snapshot of customer attributes (latest order_date).
  This is a Type 1 SCD — we overwrite; no history is tracked.
  For a Type 2 SCD you'd add a valid_from / valid_to — out of scope for v1.
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) AS row_num
    FROM source
)

SELECT
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_segment,
    customer_city,
    customer_state,
    customer_country,
    customer_street,
    customer_zipcode
FROM ranked
WHERE row_num = 1
