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
    tax
from {{ref('ga_channels_user_type')}}
left join {{ref('channel_correction')}} using (channel)