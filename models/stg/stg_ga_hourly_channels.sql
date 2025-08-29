{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    hour,
    platform,
    channel,
    sessions,
    engaged_sessions,
    transactions,
    purchase_revenue,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_hourly_channels')}}
left join {{ref('channel_correction')}} using (channel)