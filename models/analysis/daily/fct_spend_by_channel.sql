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
        channel,
        spend
    from {{ref('marketing_spend')}}
),
cte_forecast as (
    select
        date,
        channel,
        spend_forecast
    from {{ref('marketing_spend_forecast')}}
)
select
    date,
    channel,
    coalesce(spend, 0) as spend,
    coalesce(spend_forecast, 0) as spend_forecast
from cte_spend
natural full join cte_forecast
order by date, channel