select supplier_id, product_id, supplier_name, cost, perishable
from {{ ref("int_supplies") }}
