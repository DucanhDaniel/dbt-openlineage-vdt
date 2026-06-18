{{
    config(
        materialized="incremental",
        unique_key=["metric_date", "ad_id", "country", "region"],
        incremental_strategy="merge",
    )
}}

select
    -- foreign keys / dimensions
    updated_at,
    date_start as metric_date,
    ad_id,
    country,
    region,

    -- metrics
    spend,
    impressions,
    reach,
    clicks,
    cpc,
    cpm,
    frequency,
    ctr

from {{ ref("int_fb_location") }}

{% if is_incremental() %}
    where
        updated_at
        >= (select coalesce(max(tgt.updated_at), '1900-01-01') from {{ this }} tgt)
{% endif %}
