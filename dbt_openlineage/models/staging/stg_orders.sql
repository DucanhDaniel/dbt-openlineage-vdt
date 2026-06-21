select
    id as order_id,
    customer as customer_id,
    ordered_at,
    store_id,
    subtotal,
    tax_paid,
    order_total
from {{ source("jaffle", "raw_orders") }}
where id is not null
