{{
    config(
        materialized="incremental",
        unique_key=["metric_date", "ad_id", "age", "gender"],
        incremental_strategy="merge",
    )
}}

select
    -- foreign keys / dimensions
    date_start as metric_date,
    updated_at,
    ad_id,
    age,
    gender,

    -- metrics
    spend,
    impressions,
    reach,
    clicks,
    cpc,
    cpm,
    frequency,
    ctr

from {{ ref("int_fb_age_gender") }}

{% if is_incremental() %}
    where updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
