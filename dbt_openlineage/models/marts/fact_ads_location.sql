{{
    config(
        materialized="incremental",
        unique_key=["metric_date", "ad_id", "country", "region"],
        incremental_strategy="merge",
    )
}}

select
    -- foreign keys / dimensions
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
