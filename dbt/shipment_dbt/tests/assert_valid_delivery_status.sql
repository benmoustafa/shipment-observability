/*
  assert_valid_delivery_status.sql

  Checks that delivery_status only contains known values.
  Any unknown status suggests a new upstream category was added without updating
  our schema contract — exactly the kind of silent drift we want to catch.
*/

SELECT
    order_id,
    order_item_id,
    delivery_status
FROM {{ ref('fact_shipments') }}
WHERE delivery_status NOT IN (
    'Advance shipping',
    'Late delivery',
    'Shipping on time',
    'Shipping canceled'
)
