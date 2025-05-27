with 
base_customers as(
    select * from {{ ref('stg_jaffle_shop__customers') }}
),
base_orders as  (
    select * from {{ ref ('stg_jaffle_shop__orders' )}}
),
base_payments as (
    select * from {{ ref ('stg_stripe__payments') }}
),
completed_payment as (
    select 
        order_id , 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
    from base_payments
    where status <> 'fail'
    group by 1

),

paid_orders as (
    select 
        base_orders.order_id,
        base_orders.customer_id,
        base_orders.order_date as order_placed_at,
        base_orders.status as order_status,
        completed_payment.total_amount_paid,
        completed_payment.payment_finalized_date,
        base_customers.first_name as customer_first_name,
        base_customers.last_name as customer_last_name
    from base_orders 
    left join completed_payment 
        on base_orders.order_id = completed_payment.order_id
    left join base_customers using (customer_id)
),

final as(
    paid_orders.*,
    --sales transaction sequence
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    --customer sales sequence
    row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
    --new vs returning customer
    case 
        when (
            rank over(
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
            ) = 1
        ) then 'new'
        else 'return' 
    end as nvsr,
    --customer_lifetime_value
    sum(paid_orders.total_amount_paid) over (
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
    ) as customer_lifetime_value, 
    --first day of sales
    first_value(paid_orders.order_placed_at) over(
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
    ) as fdos
from paid_orders 
order by order_id
)

select * from final