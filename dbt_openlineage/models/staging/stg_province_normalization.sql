{{ config(materialized='view') }}

select
    original_province,
    standardized_province
from {{ ref('province_normalization') }}
