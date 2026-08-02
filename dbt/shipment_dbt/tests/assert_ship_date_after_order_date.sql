/*
  assert_ship_date_after_order_date.sql

  Business rule: a shipment cannot be dispatched before an order is placed.
  Returns rows that VIOLATE this constraint — dbt test passes when 0 rows returned.
*/

SELECT
    order_id,
    order_item_id,
    order_date_id,
    ship_date_id
FROM {{ ref('fact_shipments') }}
WHERE ship_date_id < order_date_id
