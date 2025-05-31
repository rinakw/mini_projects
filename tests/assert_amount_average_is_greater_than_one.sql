{{ config(enabled = false) }}
select 
    customer_id,
    avg(amount) as avg_amount
from {{ ref('fct_orders') }}
group by 1
having count(1) > 1 and avg_amount < 1