{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    channel as channel_original,
    coalesce(channel_correction, channel) as channel,
    coalesce(user_type, '(not set)') as user_type,
    sessions,
    revenue,
    purchases,
    engaged_sessions,
    shipping,
    tax,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_channels_user_type')}}
left join {{ref('channel_correction')}} using (channel)