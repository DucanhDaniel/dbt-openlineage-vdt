select
    o.order_id,
    o.customer_id,
    o.ordered_at,
    o.store_id,
    o.subtotal,
    o.tax_paid,
    o.order_total as total,
    p.product_id,
    i.item_id
from {{ ref("stg_items") }} as i
left join {{ ref("stg_orders") }} as o on o.order_id = i.order_id
left join {{ ref("stg_products") }} as p on i.product_id = p.product_id
