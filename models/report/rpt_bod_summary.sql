{{
    config(
        materialized = 'view'
    )
}}
with
cte_calendar as (
    select
        date,
        week_id,
        month_id,
        quarter_id,
        year_id
    from {{ref('dim_date')}}
    where
        year_id >= 2024
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
cte_demand as (
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
),
cte_demand_yoy as (
    select
        date,
        time_period,
        orders,
        sale_qty,
        sale_cost,
        sale_amt,
        sessions,
        engaged_sessions,
        orders_forecast,
        sale_qty_forecast,
        sale_amt_forecast,
        sessions_forecast,
        orders_budget,
        sale_qty_budget,
        sale_amt_budget,
        sessions_budget
    from cte_demand
    join cte_date_xfrm using (xfrm_date)
),
cte_budget as (
    select
        date,
        time_period,
        net_sales,
        cogs,
        shipping_revenue,
        net_sales_budget,
        cogs_budget,
        shipping_revenue_budget
    from {{ref('fct_budget')}}
)
select
    time_period,
    month_id,
    quarter_id,
    year_id,
    min(date) as min_date,
    max(date) as max_date,
    sum(orders) as orders,
    sum(sale_qty) as sale_qty,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt,
    sum(sessions) as sessions,
    sum(engaged_sessions) as engaged_sessions,
    sum(orders_forecast) as orders_forecast,
    sum(sale_qty_forecast) as sale_qty_forecast,
    sum(sale_amt_forecast) as sale_amt_forecast,
    sum(sessions_forecast) as sessions_forecast,
    sum(orders_budget) as orders_budget,
    sum(sale_qty_budget) as sale_qty_budget,
    sum(sale_amt_budget) as sale_amt_budget,
    sum(sessions_budget) as sessions_budget,
    sum(net_sales) as net_sales,
    sum(net_sales) - sum(cogs) as gross_margin,
    sum(shipping_revenue) as shipping_revenue,
    sum(net_sales_budget) as net_sales_budget,
    sum(net_sales_budget) - sum(cogs_budget) as gross_margin_budget,
    sum(shipping_revenue_budget) as shipping_revenue_budget
from cte_calendar
natural join cte_demand_yoy
natural left join cte_budget
where
    date < current_date()
group by all