-- calculate more metrics for analysis
select
    fl.created_at,
    fl.updated_at,
    fl.date_start,
    fl.date_stop,

    fl.account_id,
    fl.account_name,
    fl.ad_id,
    fl.ad_name,
    fl.creative_id,
    fl.creative_name,
    fl.country,
    pn.standardized_province as region,

    fl.spend,
    fl.impressions,
    fl.reach,
    fl.clicks,
    fl.cpc,
    fl.cpm,
    fl.frequency,

    -- calculate more metrics
    coalesce((1.0 * fl.clicks / nullif(fl.impressions, 0)) * 100, 0) as ctr

from {{ ref("stg_fb_location") }} as fl
inner join
    {{ ref("stg_province_normalization") }} as pn
    on fl.region = pn.original_province
