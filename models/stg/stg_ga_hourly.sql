{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    hour,
    sessions,
    engaged_sessions,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_hourly')}}