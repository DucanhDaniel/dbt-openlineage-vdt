with
    unioned as (
        select
            ad_id,
            ad_name,
            account_id,
            account_name,
            creative_id,
            creative_name,
            creative_link
        from {{ ref("int_fb_age_gender") }}

        union all

        select
            ad_id,
            ad_name,
            account_id,
            account_name,
            creative_id,
            creative_name,
            null as creative_link
        from {{ ref("int_fb_location") }}
    ),

    deduped as (
        select
            ad_id,
            ad_name,
            account_id,
            account_name,
            creative_id,
            creative_name,
            creative_link,
            row_number() over (
                partition by ad_id order by creative_link desc nulls last
            ) as rn
        from unioned
    )

select
    ad_id, ad_name, account_id, account_name, creative_id, creative_name, creative_link
from deduped
where rn = 1
