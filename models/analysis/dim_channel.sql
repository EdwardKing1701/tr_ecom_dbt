{{
    config(
        materialized = 'table',
        pk = ['channel']
    )
}}
select
    channel,
    channel_group,
    current_timestamp() as inserted_ts
from {{ref('channel_group')}}
order by channel