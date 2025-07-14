{{
    config(
        materialized = 'table',
        pk = ['date', 'channel']
    )
}}
with
cte_spend as (
    select
        date,
        coalesce(channel_correction, channel) as channel,
        sum(spend) as spend
    from {{ref('marketing_spend')}}
    left join {{ref('channel_correction')}} using (channel)
    group by all
),
cte_forecast as (
    select
        date,
        coalesce(channel_correction, channel) as channel,
        sum(spend_forecast) as spend_forecast
    from {{ref('marketing_spend_forecast')}}
    left join {{ref('channel_correction')}} using (channel)
    group by all
)
select
    date,
    channel,
    coalesce(spend, 0) as spend,
    coalesce(spend_forecast, 0) as spend_forecast
from cte_spend
natural full join cte_forecast
order by date, channel