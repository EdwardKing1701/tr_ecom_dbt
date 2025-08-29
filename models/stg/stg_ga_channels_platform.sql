{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    channel as channel_original,
    coalesce(channel_correction, channel) as channel,
    coalesce(platform, '(not set)') as platform,
    sessions,
    revenue,
    purchases,
    engaged_sessions,
    shipping,
    tax,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_channels_platform')}}
left join {{ref('channel_correction')}} using (channel)