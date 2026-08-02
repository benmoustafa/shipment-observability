{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

/*
  fact_shipments.sql
  Grain: one row per order line item (order_id, order_item_id).

  Grain decision rationale:
    We chose order-line grain (not order grain) because each line item can
    have a different product, quantity, discount, and contribution to profit.
    Aggregating to order grain before the fact table would lose this detail
    and make per-product analysis impossible without a separate table.
    Dashboards that need order-level aggregations (total order value) can
    GROUP BY order_id — that direction is always safe.

  Foreign keys:
    - customer_id  → dim_customers.customer_id
    - product_id   → dim_products.product_id
    - order_date   → dim_dates.date_id  (date of order placement)
    - ship_date    → dim_dates.date_id  (date of shipment)

  Measures:
    - sales               Revenue for this line item
    - order_item_total    Total inc. discounts
    - order_profit        Profit at order level (may be negative = loss)
    - benefit_per_order   Margin contribution
    - order_item_quantity Quantity shipped
    - days_shipping_real  Actual days to ship
    - is_late             Binary late-delivery flag (from date arithmetic)
    - days_late           Extra days beyond scheduled (0 if on time)
*/

WITH order_items AS (
    SELECT * FROM {{ ref('int_order_items') }}
)

SELECT
    -- === Surrogate key (no UUID needed — natural composite key is unique) ===
    order_id,
    order_item_id,

    -- === Foreign keys ===
    customer_id,
    product_id,
    category_id,
    department_id,
    order_date                AS order_date_id,
    ship_date                 AS ship_date_id,

    -- === Degenerate dimensions (stored on fact, not worth their own dim) ===
    order_status,
    transaction_type,
    delivery_status,
    shipping_mode,
    order_region,
    order_country,
    market,

    -- === Measures ===
    sales,
    order_item_total,
    order_profit,
    benefit_per_order,
    order_item_discount,
    order_item_discount_rate,
    order_item_product_price,
    order_item_profit_ratio,
    order_item_quantity,
    days_shipping_real,
    days_shipping_scheduled,
    days_late,

    -- === Quality flags ===
    is_late,
    late_delivery_risk_flag

FROM order_items
