{% set old_etl_relation = ref('customer_orders') %}
{% set new_etl_relation = ref('fct_customer_order') %}

{{ audit_helper.compare_relations (
    a_relation = old_etl_relation,
    b_relation = new_etl_relation,
    primary_key ='order_id'
)
}}