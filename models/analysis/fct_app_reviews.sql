{{
    config(
        materialized = 'table',
        pk = ['review_id']
    )
}}
select
    review_id,
    created_ts,
    created_date,
    reviewer_nickname,
    rating,
    title as review_title,
    body as review_body,
    source_synced_ts,
    current_timestamp() as inserted_ts
from {{ref('stg_itunes_connect_review')}}
order by created_ts, review_id