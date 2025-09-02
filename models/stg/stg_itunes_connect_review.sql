{{
    config(
        materialized = 'view'
    )
}}
select
    id as review_id,
    convert_timezone('America/Los_Angeles', created_date) as created_ts,
    created_ts::date as created_date,
    territory,
    reviewer_nickname,
    rating,
    title,
    body,
    convert_timezone('America/Los_Angeles', _fivetran_synced) as source_synced_ts
from {{source('itunes_connect', 'app_store_review')}}