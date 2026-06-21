select supplier_id, supplier_name, cost, perishable, product_id
from {{ ref("stg_supplies") }}
