{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    platform,
    users,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_add_to_cart_users')}}