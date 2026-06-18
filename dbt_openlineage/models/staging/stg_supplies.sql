select id as supplier_id, name as supplier_name, cost, perishable, sku as product_id
from {{ source("jaffle", "raw_supplies") }}
where id is not null
