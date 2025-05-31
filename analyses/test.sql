SELECT 
    rank() over(
        partition by customer_id
        order by order_date, order_id)
FROM  {{ ref ('stg_jaffle_shop__orders' )}}