{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    platform,
    sessions,
    active_users,
    first_opens,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_app_users')}}