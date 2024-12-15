WITH customers AS (
  SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    COUNT_LIFETIME_ORDERS,
    FIRST_ORDERED_AT,
    LAST_ORDERED_AT,
    LIFETIME_SPEND_PRETAX,
    LIFETIME_TAX_PAID,
    LIFETIME_SPEND,
    CUSTOMER_TYPE
  FROM {{ ref('customers') }}
), orders AS (
  SELECT
    ORDER_ID,
    LOCATION_ID,
    CUSTOMER_ID,
    SUBTOTAL_CENTS,
    TAX_PAID_CENTS,
    ORDER_TOTAL_CENTS,
    SUBTOTAL,
    TAX_PAID,
    ORDER_TOTAL,
    ORDERED_AT,
    ORDER_COST,
    ORDER_ITEMS_SUBTOTAL,
    COUNT_FOOD_ITEMS,
    COUNT_DRINK_ITEMS,
    COUNT_ORDER_ITEMS,
    IS_FOOD_ORDER,
    IS_DRINK_ORDER,
    CUSTOMER_ORDER_NUMBER
  FROM {{ ref('orders') }}
), join_1 AS (
  SELECT
    orders.ORDER_ID,
    orders.LOCATION_ID,
    orders.CUSTOMER_ID,
    orders.SUBTOTAL_CENTS,
    orders.TAX_PAID_CENTS,
    orders.ORDER_TOTAL_CENTS,
    orders.SUBTOTAL,
    orders.TAX_PAID,
    orders.ORDER_TOTAL,
    orders.ORDERED_AT,
    orders.ORDER_COST,
    orders.ORDER_ITEMS_SUBTOTAL,
    orders.COUNT_FOOD_ITEMS,
    orders.COUNT_DRINK_ITEMS,
    orders.COUNT_ORDER_ITEMS,
    orders.IS_FOOD_ORDER,
    orders.IS_DRINK_ORDER,
    orders.CUSTOMER_ORDER_NUMBER,
    customers.CUSTOMER_ID AS CUSTOMER_ID_1,
    customers.CUSTOMER_NAME,
    customers.COUNT_LIFETIME_ORDERS,
    customers.FIRST_ORDERED_AT,
    customers.LAST_ORDERED_AT,
    customers.LIFETIME_SPEND_PRETAX,
    customers.LIFETIME_TAX_PAID,
    customers.LIFETIME_SPEND,
    customers.CUSTOMER_TYPE
  FROM orders
  JOIN customers
    ON orders.CUSTOMER_ID = customers.CUSTOMER_ID
)
SELECT
  *
FROM join_1