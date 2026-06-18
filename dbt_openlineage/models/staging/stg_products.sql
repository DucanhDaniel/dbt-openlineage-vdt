select sku as product_id, name as product_name, type, price, description
from {{ source("jaffle", "raw_products") }}
where sku is not null
