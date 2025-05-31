select
    id as payment_id,
    order_id,
    payment_method,
    status,
    -- amount is stored in cents, convert it to dollars
    {{cent_to_dollar(col_name='amount')}} as amount,
    created as created_at

from raw.stripe.payment 