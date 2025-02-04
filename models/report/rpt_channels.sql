{{
    config(
        materialized = 'view'
    )
}}
with
cte_channels as (
    select * exclude (inserted_ts)
    from {{ref('fct_sessions_by_channel')}}
),
cte_forecast as (
    select * exclude (source_synced_ts, inserted_ts)
    from {{ref('fct_forecast_by_channel')}}
)
select
    date,
    week_id,
    month_id,
    quarter_id,
    year_id,
    channel,
    coalesce(sessions, 0) as sessions,
    coalesce(engaged_sessions, 0) as engaged_sessions,
    coalesce(analytics_demand, 0) as analytics_demand,
    coalesce(adjusted_demand, 0) as adjusted_demand,
    coalesce(analytics_orders, 0) as analytics_orders,
    coalesce(adjusted_orders, 0) as adjusted_orders,
    coalesce(analytics_units, 0) as analytics_units,
    coalesce(adjusted_units, 0) as adjusted_units,
    coalesce(analytics_shipping, 0) as analytics_shipping,
    coalesce(adjusted_shipping, 0) as adjusted_shipping,
    coalesce(analytics_tax, 0) as analytics_tax,
    coalesce(adjusted_tax, 0) as adjusted_tax,
    coalesce(forecast, 0) as forecast,
    coalesce(budget, 0) as budget
from cte_channels
full join cte_forecast using(date, channel)
join {{ref('dim_date')}} using(date)