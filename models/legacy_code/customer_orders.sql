with paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name    as customer_first_name,
        c.last_name as customer_last_name
    from {{ ref('stg_jaffle_shop__orders') }} as orders
    left join (
        select 
            order_id , 
            max(created) as payment_finalized_date, 
            sum(amount) / 100.0 as total_amount_paid
        from {{ source('stripe', 'payment') }}
        where status <> 'fail'
        group by 1) p 
        on orders.order_id = p.order_id
    left join {{ ref('stg_jaffle_shop__customers') }} c 
        on orders.customer_id = c.customer_id 
),
customer_orders as (
    select 
        c.customer_id
        , min(order_date) as first_order_date
        , max(order_date) as most_recent_order_date
        , count(orders.order_id) as number_of_orders
    from {{ ref('stg_jaffle_shop__customers') }} c 
    left join {{ ref('stg_jaffle_shop__orders') }} as orders
        on orders.customer_id = c.customer_id 
    group by 1
)

select
    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
    case when c.first_order_date = p.order_placed_at
    then 'new'
    else 'return' end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
from paid_orders p
left join customer_orders as c using (customer_id)
left outer join (
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
    ) x 
    on x.order_id = p.order_id
order by order_id
