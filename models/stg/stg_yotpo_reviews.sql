{{
    config(
        materialized = 'view'
    )
}}
select
    id as review_id,
    convert_timezone('America/Los_Angeles', created_at) as created_ts,
    created_ts::date as created_date,
    published as is_published,
    analysis.email_address(user_email) as email_address,
    title as review_title,
    content as review_body,
    score as rating,
    nullif(external_order_id, '') as order_id, -- need to ensure integrity
    external_product_id as product_id, -- need to ensure integrity
    votes_up,
    votes_down,
    is_incentivized,
    nullif(incentive_type, '') as incentive_type,
    verified_buyer as is_verified_buyer,
    convert_timezone('America/Los_Angeles', _fivetran_synced) as source_synced_ts
from {{source('yotpo', 'review')}}
where
    not _fivetran_deleted