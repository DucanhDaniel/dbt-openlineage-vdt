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
    creative_link,

    -- age gender breakdowns
    age,
    gender,

    -- metrics
    coalesce(spend, 0) as spend,
    coalesce(impressions, 0) as impressions,
    coalesce(reach, 0) as reach,
    coalesce(clicks, 0) as clicks,
    coalesce(cpc, 0) as cpc,
    coalesce(cpm, 0) as cpm,
    coalesce(frequency, 0) as frequency,

    -- calculate more metrics
    coalesce((1.0 * clicks / nullif(impressions, 0)) * 100, 0) as ctr

{# 1 / 0 as error_column #}
from {{ ref("stg_fb_age_gender") }}
