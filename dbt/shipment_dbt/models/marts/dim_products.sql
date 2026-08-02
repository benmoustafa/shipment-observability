{{
    config(
        materialized='table',
        tags=['marts', 'dim']
    )
}}

/*
  dim_products.sql
  Grain: one row per unique product_id.
  Latest attributes via row_number, same Type 1 SCD pattern as dim_customers.
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY order_date DESC
        ) AS row_num
    FROM source
)

SELECT
    product_id,
    product_name,
    product_price,
    product_status,
    category_id,
    category_name,
    department_id,
    department_name
FROM ranked
WHERE row_num = 1
