/*
  assert_no_negative_shipping_quantity.sql

  Business rule: order item quantity must always be a positive integer.
  A zero or negative quantity indicates a data entry error or bad ETL.
*/

SELECT
    order_id,
    order_item_id,
    order_item_quantity
FROM {{ ref('fact_shipments') }}
WHERE order_item_quantity <= 0
