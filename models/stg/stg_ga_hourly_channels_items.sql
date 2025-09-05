{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    hour,
    platform,
    channel as channel_original,
    coalesce(channel_correction, channel) as channel,
    quantity,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_hourly_channels_items')}}
left join {{ref('channel_correction')}} using (channel)