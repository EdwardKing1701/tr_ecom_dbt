{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    channel as channel_original,
    coalesce(channel_correction, channel) as channel,
    quantity
from {{ref('ga_items')}}
left join {{ref('channel_correction')}} using (channel)