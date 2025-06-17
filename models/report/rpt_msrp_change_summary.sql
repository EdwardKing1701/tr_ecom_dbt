{{
    config(
        materialized = 'view'
    )
}}
with
cte_calendar as (
    select
        date,
        xfrm_date,
        time_period
    from {{ref('date_xfrm')}}
    join {{ref('dim_date')}} using (date)
    where
        to_date_type = 'TODAY'
        and time_period in ('TY', 'LY')
        and date between '2025-03-30' and previous_day(current_date(), 'sa')
),
cte_msrp_change as (
    select
        date,
        msrp_change
    from {{ref('temp_msrp_change')}}
),
cte_kpi as (
    select
        date as xfrm_date,
        orders,
        sale_qty,
        sale_cost,
        sale_amt,
        sessions,
        orders_forecast,
        sale_qty_forecast,
        sale_amt_forecast,
        sessions_forecast
    from {{ref('rpt_daily_kpi')}}
)
select
    date,
    time_period,
    coalesce(msrp_change, 'pre-MSRP changes') as msrp_change,
    orders,
    sale_qty,
    sale_cost,
    sale_amt,
    sessions,
    orders_forecast,
    sale_qty_forecast,
    sale_amt_forecast,
    sessions_forecast
from cte_kpi
join cte_calendar using (xfrm_date)
left join cte_msrp_change using (date)