select
    user_id,
    "createdAt" as created_at,
    "updatedAt" as updated_at,
    date_start,
    date_stop,
    account_id,
    account_name,
    id as ad_id,
    name as ad_name,
    creative_id,
    creative_name,
    creative_link,

    -- age gender breakdowns
    age,
    gender,

    -- metrics
    spend,
    impressions,
    reach,
    clicks,
    cpc,
    cpm,
    frequency

from {{ source("facebook", "fad_age_gender_detailed_report") }}
