{{
    config(
        materialized = 'table',
        pk = ['date', 'platform']
    )
}}
select
    date,
    platform,
    users,
    source_synced_ts,
    current_timestamp() as inserted_ts
from {{ref('src_ga_add_to_cart_users')}}
order by date, platform