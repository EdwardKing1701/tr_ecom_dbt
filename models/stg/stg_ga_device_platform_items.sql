{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    platform,
    device_category,
    quantity,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_device_platform_items')}}