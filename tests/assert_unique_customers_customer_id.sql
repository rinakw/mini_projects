{{ config(enabled = false) }}
select 
    customer_id
from {{ ref('fct_orders') }}
group by 1
having count(1) > 1