select
    item_id,
    order_id,
    customer_id,
    product_id,
    store_id,
    ordered_at,
    tax_paid,
    total,
    subtotal
from {{ ref("int_order_product") }}
