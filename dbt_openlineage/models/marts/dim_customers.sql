select customer_id, customer_name from {{ ref("int_customers") }}
