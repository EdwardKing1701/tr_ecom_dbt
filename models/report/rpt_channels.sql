{{
    config(
        materialized = 'view'
    )
}}
with
cte_channels as (
    select
        date,
        channel,
        sessions,
        engaged_sessions,
        orders,
        sale_qty,
        sale_amt,
        sessions_unadjusted,
        engaged_sessions_unadjusted,
        orders_unadjusted,
        sale_qty_unadjusted,
        sale_amt_unadjusted
    from {{ref('fct_sessions_by_channel')}}
),
cte_forecast as (
    select
        date,
        channel,
        forecast,
        budget
    from {{ref('v_fct_forecast_by_channel')}}
),
cte_spend as (
    select
        date,
        channel,
        spend,
        spend_forecast
    from {{ref('fct_spend_by_channel')}}
),
cte_year_id as (
    select
        date,
        year_id
    from {{ref('dim_date')}}
)
select
    date,
    year_id,
    channel,
    sessions,
    coalesce(engaged_sessions, 0) as engaged_sessions,
    coalesce(orders, 0) as orders,
    coalesce(sale_qty, 0) as sale_qty,
    coalesce(sale_amt, 0) as sale_amt,
    coalesce(sessions_unadjusted, 0) as sessions_unadjusted,
    coalesce(engaged_sessions_unadjusted, 0) as engaged_sessions_unadjusted,
    coalesce(orders_unadjusted, 0) as orders_unadjusted,
    coalesce(sale_qty_unadjusted, 0) as sale_qty_unadjusted,
    coalesce(sale_amt_unadjusted, 0) as sale_amt_unadjusted,
    coalesce(forecast, 0) as sale_amt_forecast,
    coalesce(budget, 0) as sale_amt_budget,
    coalesce(spend, 0) as spend,
    coalesce(spend_forecast, 0) as spend_forecast
from cte_channels
natural full join cte_forecast
natural full join cte_spend
natural join cte_year_id