select id as item_id, order_id, sku as product_id
from {{ source("jaffle", "raw_items") }}
where id is not null
