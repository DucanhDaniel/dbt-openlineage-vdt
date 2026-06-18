select store_id, store_name, opened_at, tax_rate from {{ ref("stg_stores") }}
