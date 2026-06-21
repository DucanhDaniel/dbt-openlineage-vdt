select product_id, product_name, type, price, description from {{ ref("stg_products") }}
