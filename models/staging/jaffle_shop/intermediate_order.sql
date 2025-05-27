with 
customers as(
    select 
        customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from {{ ref('stg_jaffle_shop__customers') }}
),
orders as  (
    select 
        order_id,
        customer_id,
        order_date as order_placed_at,
        status as order_status
        from {{ ref ('stg_jaffle_shop__orders' )}}
),
payments as (
    select * from {{ ref ('stg_stripe__payments') }}
),
completed_payment as (
    select 
        order_id , 
        max(created) as payment_finalized_date, 
        sum(amount) as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        completed_payment.total_amount_paid,
        completed_payment.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name
    from orders 
    left join completed_payment 
        on orders.order_id = completed_payment.order_id
    left join customers using (customer_id)
),

select * from paid_orders