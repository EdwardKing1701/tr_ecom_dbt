{{
    config(
        materialized = 'table',
        pk = ['date', 'channel']
    )
}}
select
    date,
    channel,
    coalesce(spend, 0) as spend
from {{ref('marketing_spend')}}
order by date, channel