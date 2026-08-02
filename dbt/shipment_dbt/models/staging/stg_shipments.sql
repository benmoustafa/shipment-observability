{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

/*
  stg_shipments.sql — Staging model for raw DataCo Supply Chain data.

  Responsibilities at this layer:
    1. Rename all columns from raw mixed-case + spaces → clean snake_case.
    2. Explicit CAST for every column — no implicit type coercion surprises.
    3. Derive `is_late` flag from the actual delivery status string
       (NOT from copying `late_delivery_risk` — we validate the two agree in tests).
    4. Nothing else. No joins, no aggregations, no business rules.

  Source: raw_shipments table loaded by ingestion/loader.py
*/

SELECT
    -- === Order identifiers ===
    CAST(`Order Id`              AS UNSIGNED)       AS order_id,
    CAST(`Order Customer Id`     AS UNSIGNED)       AS order_customer_id,
    CAST(`Order Item Id`         AS UNSIGNED)       AS order_item_id,

    -- === Dates ===
    STR_TO_DATE(`order date (DateOrders)`,   '%m/%d/%Y %H:%i')   AS order_date,
    STR_TO_DATE(`shipping date (DateOrders)`,'%m/%d/%Y %H:%i')   AS ship_date,

    -- === Shipping / delivery ===
    CAST(`Days for shipping (real)`        AS UNSIGNED)           AS days_shipping_real,
    CAST(`Days for shipment (scheduled)`   AS UNSIGNED)           AS days_shipping_scheduled,
    TRIM(`Delivery Status`)                                        AS delivery_status,
    CAST(`Late_delivery_risk`              AS UNSIGNED)           AS late_delivery_risk_flag,
    TRIM(`Shipping Mode`)                                          AS shipping_mode,
    TRIM(`Type`)                                                   AS transaction_type,
    TRIM(`Order Status`)                                           AS order_status,

    -- Derived: is_late — TRUE when the real days exceed scheduled days.
    -- We intentionally derive this from the date arithmetic, NOT from copying
    -- late_delivery_risk. The `assert_late_flag_consistency` dbt test will
    -- verify these two agree. Disagreements = upstream logic bug.
    CASE
        WHEN `Days for shipping (real)` > `Days for shipment (scheduled)` THEN 1
        ELSE 0
    END                                                            AS is_late,

    -- === Financials ===
    CAST(`Sales`                           AS DECIMAL(12,2))      AS sales,
    CAST(`Order Item Total`                AS DECIMAL(12,2))      AS order_item_total,
    CAST(`Order Profit Per Order`          AS DECIMAL(12,2))      AS order_profit,
    CAST(`Benefit per order`               AS DECIMAL(12,2))      AS benefit_per_order,
    CAST(`Order Item Discount`             AS DECIMAL(12,2))      AS order_item_discount,
    CAST(`Order Item Discount Rate`        AS DECIMAL(8,4))       AS order_item_discount_rate,
    CAST(`Order Item Product Price`        AS DECIMAL(12,2))      AS order_item_product_price,
    CAST(`Order Item Profit Ratio`         AS DECIMAL(8,4))       AS order_item_profit_ratio,
    CAST(`Order Item Quantity`             AS UNSIGNED)           AS order_item_quantity,
    CAST(`Sales per customer`              AS DECIMAL(12,2))      AS sales_per_customer,

    -- === Customer ===
    CAST(`Customer Id`                     AS UNSIGNED)           AS customer_id,
    TRIM(`Customer Fname`)                                        AS customer_first_name,
    TRIM(`Customer Lname`)                                        AS customer_last_name,
    TRIM(`Customer Email`)                                        AS customer_email,
    TRIM(`Customer Segment`)                                      AS customer_segment,
    TRIM(`Customer City`)                                         AS customer_city,
    TRIM(`Customer State`)                                        AS customer_state,
    TRIM(`Customer Country`)                                      AS customer_country,
    TRIM(`Customer Street`)                                       AS customer_street,
    CAST(`Customer Zipcode`                AS DECIMAL(10,0))      AS customer_zipcode,

    -- === Product ===
    CAST(`Product Card Id`                 AS UNSIGNED)           AS product_id,
    TRIM(`Product Name`)                                          AS product_name,
    CAST(`Product Price`                   AS DECIMAL(12,2))      AS product_price,
    CAST(`Product Status`                  AS UNSIGNED)           AS product_status,

    -- === Category / Department ===
    CAST(`Category Id`                     AS UNSIGNED)           AS category_id,
    TRIM(`Category Name`)                                         AS category_name,
    CAST(`Department Id`                   AS UNSIGNED)           AS department_id,
    TRIM(`Department Name`)                                       AS department_name,

    -- === Geography ===
    TRIM(`Market`)                                                AS market,
    TRIM(`Order Region`)                                          AS order_region,
    TRIM(`Order Country`)                                         AS order_country,
    TRIM(`Order State`)                                           AS order_state,
    TRIM(`Order City`)                                            AS order_city,
    CAST(`Latitude`                        AS DECIMAL(10,6))      AS latitude,
    CAST(`Longitude`                       AS DECIMAL(10,6))      AS longitude

FROM {{ source('raw', 'raw_shipments') }}
