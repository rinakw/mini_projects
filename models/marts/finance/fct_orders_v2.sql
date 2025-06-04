with 
orders as  (
    select * from {{ ref ('stg_jaffle_shop__orders' )}}),

payments as (
    select * from {{ ref ('stg_stripe__payments') }}),

order_payments as (

    select
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from payments
    group by 1),

final as (
    select
        cast(orders.order_id as varchar) as order_id,
        cast(orders.customer_id  as varchar) as customer_id,
        orders.order_date as location_ordered_at,
        coalesce (order_payments.amount, 0) as order_amount

    from orders
    left join order_payments using (order_id))

select * from final
