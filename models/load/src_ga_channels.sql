{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    channel as channel_original,
    coalesce(channel_correction, channel) as channel,
    sessions,
    revenue,
    purchases,
    engaged_sessions,
    shipping,
    tax
from {{ref('ga_channels')}}
left join {{ref('channel_correction')}} using (channel)