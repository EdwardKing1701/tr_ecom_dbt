{{
    config(
        materialized = 'view'
    )
}}
with
cte_calendar as (
    select
        date,
        date as min_date,
        date as max_date,
        week_id,
        month_id,
        quarter_id,
        year_id
    from {{ref('dim_date')}}
),
cte_date_xfrm as (
    select
        date,
        time_period,
        xfrm_date
    from {{ref('date_xfrm')}}
    where
        to_date_type = 'TODAY'
        and time_period in ('TY', 'LY')
),
cte_kpi as (
    select
        date as xfrm_date,
        orders,
        sale_qty,
        sale_cost,
        sale_amt,
        shipping,
        tax,
        net_sale_qty,
        net_sale_cost,
        net_sale_amt,
        gross_margin,
        gross_sale_qty,
        gross_sale_cost,
        gross_sale_amt,
        return_qty,
        return_cost,
        return_amt,
        sessions,
        engaged_sessions,
        orders_forecast,
        sale_qty_forecast,
        sale_amt_forecast,
        sessions_forecast,
        orders_budget,
        sale_qty_budget,
        sale_amt_budget,
        sessions_budget,
        net_sale_amt_budget,
        net_sale_cost_budget,
        shipping_budget,
        net_sale_amt + shipping as net_sale_and_shipping_amt,
        net_sale_amt_budget + shipping_budget as net_sale_and_shipping_amt_budget
    from {{ref('rpt_daily_kpi')}}
)
select
    * exclude (xfrm_date)    
from cte_calendar
natural join cte_date_xfrm
natural join cte_kpi
where
    date < current_date()