{%- set payment_method = ['credit_card','coupon', 'bank_transfer', 'gift_card'] -%}

with 
payment as (
    select * from {{ ref('stg_stripe__payments') }}
),
pivoted as (
select
    order_id, 
    {% for payment_method in payment_method -%}
    sum(case when payment_method = '{{payment_method}}' then amount else 0 end) as payment_{{payment_method}}
    {%- if not loop.last%} 
    ,
    {% endif -%}
    {% endfor %}
from payment
group by 1
)

select * from pivoted