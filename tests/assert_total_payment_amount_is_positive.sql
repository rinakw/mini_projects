
select 
    customer_id, 
    amount
from {{ ref('fct_orders') }}
where amount < 0