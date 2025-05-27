SELECT 
    c.id AS customer_id,
    MIN(orders.order_date) AS first_order_date,
    MAX(orders.order_date) AS most_recent_order_date,
    COUNT(orders.id) AS number_of_orders
FROM {{ source('jaffle_shop', 'customers') }} AS c
LEFT JOIN {{ source('jaffle_shop', 'orders') }} AS orders
    ON orders.customer_id = c.id
GROUP BY c.id
