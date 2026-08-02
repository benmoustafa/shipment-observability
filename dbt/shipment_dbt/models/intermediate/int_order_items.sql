{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

/*
  int_order_items.sql — Deduplicated order-item grain.

  Why this exists: The raw dataset has one row per order-item (Order Item Id),
  but some order_ids appear across multiple rows (one per line item). This model
  keeps all line items but ensures uniqueness on (order_id, order_item_id) and
  pre-calculates key shipment metrics needed by the fact table.

  Grain: one row per (order_id, order_item_id)
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

/*
  Deduplicate: in the rare case of exact duplicate rows (same order_id,
  order_item_id), keep only the first occurrence. ROW_NUMBER avoids
  duplicating revenue metrics in the fact table.
*/
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id
            ORDER BY order_date
        ) AS row_num
    FROM source
),

final AS (
    SELECT
        order_id,
        order_item_id,
        order_customer_id,
        order_date,
        ship_date,
        order_status,
        transaction_type,
        delivery_status,
        shipping_mode,
        is_late,
        late_delivery_risk_flag,
        days_shipping_real,
        days_shipping_scheduled,

        -- Derived: how many extra days beyond the scheduled window?
        -- Cast to SIGNED before subtracting — UNSIGNED underflows when real < scheduled.
        GREATEST(
            CAST(days_shipping_real AS SIGNED) - CAST(days_shipping_scheduled AS SIGNED),
            0
        ) AS days_late,

        sales,
        order_item_total,
        order_profit,
        benefit_per_order,
        order_item_discount,
        order_item_discount_rate,
        order_item_product_price,
        order_item_profit_ratio,
        order_item_quantity,
        customer_id,
        product_id,
        category_id,
        department_id,
        order_region,
        order_country,
        market

    FROM deduped
    WHERE row_num = 1
)

SELECT * FROM final
