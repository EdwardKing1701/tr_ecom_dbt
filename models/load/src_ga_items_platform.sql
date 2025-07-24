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
    quantity
from {{ref('ga_items_platform')}}
left join {{ref('channel_correction')}} using (channel)