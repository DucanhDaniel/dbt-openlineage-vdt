-- calculate more metrics for analysis
select
    created_at,
    updated_at,
    date_start,
    date_stop,

    account_id,
    account_name,
    ad_id,
    ad_name,
    creative_id,
    creative_name,
    country,
    region,

    spend,
    impressions,
    reach,
    clicks,
    cpc,
    cpm,
    frequency,

    -- calculate more metrics
    coalesce((1.0 * clicks / nullif(impressions, 0)) * 100, 0) as ctr

from {{ ref("stg_fb_location") }}
