{{
    config(
        materialized = 'table',
        pk = ['date', 'channel']
    )
}}
select
    date,
    channel,
    coalesce(spend, 0) as spend,
    coalesce(spend_forecast, 0) as spend_forecast
from {{ref('marketing_spend')}}
order by date, channel