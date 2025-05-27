with 
intermediate as (
    select * from {{ ref('intermediate_order') }}
),
final as(
    select 
        *,
        --sales transaction sequence
        row_number() over (order by order_id) as transaction_seq,
        --customer sales sequence
        row_number() over (partition by customer_id order by order_id) as customer_sales_seq,
        --new vs returning customer
        case 
            when (
                rank over(
                    partition by customer_id
                    order by order_placed_at, order_id
                ) = 1
            ) then 'new'
            else 'return' 
        end as nvsr,
        --customer_lifetime_value
        sum(total_amount_paid) over (
            partition by customer_id
            order by order_placed_at
        ) as customer_lifetime_value, 
        --first day of sales
        first_value(order_placed_at) over(
            partition by customer_id
            order by order_placed_at
        ) as fdos
    from intermediate 
    order by order_id
)

select * from final