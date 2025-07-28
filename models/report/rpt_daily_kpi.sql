{{
    config(
        materialized = 'view'
    )
}}
with
cte_calendar as (
    select
        date
    from {{ref('dim_date')}}
),
cte_demand_sales as (
    select
        date,
        count(distinct order_id) as orders,
        sum(sale_qty) as sale_qty,
        sum(sale_cost) as sale_cost,
        sum(sale_amt) as sale_amt,
        sum(shipping) as shipping,
        sum(tax) as tax
    from {{ref('v_fct_orders')}}
    where
        date < current_date()
    group by all
),
cte_net_sales as (
    select
        date,
        net_sale_qty,
        net_sale_cost,
        net_sale_amt,
        gross_sale_qty,
        gross_sale_cost,
        gross_sale_amt,
        return_qty,
        return_cost,
        return_amt
    from {{ref('fct_net_sales')}}
    where
        date < current_date()
),
cte_sessions as (
    select
        date,
        sessions,
        engaged_sessions
    from {{ref('fct_sessions_by_day')}}
),
cte_forecast as (
    select
        date,
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
        shipping_budget
    from {{ref('fct_forecast_by_day')}}
)
select
    date,
    coalesce(orders, 0) as orders,
    coalesce(sale_qty, 0) as sale_qty,
    coalesce(sale_cost, 0) as sale_cost,
    coalesce(sale_amt, 0) as sale_amt,
    coalesce(shipping, 0) as shipping,
    coalesce(tax, 0) as tax,
    coalesce(net_sale_qty, 0) as net_sale_qty,
    coalesce(net_sale_cost, 0) as net_sale_cost,
    coalesce(net_sale_amt, 0) as net_sale_amt,
    net_sale_amt - net_sale_cost as gross_margin,
    coalesce(gross_sale_qty, 0) as gross_sale_qty,
    coalesce(gross_sale_cost, 0) as gross_sale_cost,
    coalesce(gross_sale_amt, 0) as gross_sale_amt,
    coalesce(return_qty, 0) as return_qty,
    coalesce(return_cost, 0) as return_cost,
    coalesce(return_amt, 0) as return_amt,
    coalesce(sessions, 0) as sessions,
    coalesce(engaged_sessions, 0) as engaged_sessions,
    coalesce(orders_forecast, 0) as orders_forecast,
    coalesce(sale_qty_forecast, 0) as sale_qty_forecast,
    coalesce(sale_amt_forecast, 0) as sale_amt_forecast,
    coalesce(sessions_forecast, 0) as sessions_forecast,
    coalesce(orders_budget, 0) as orders_budget,
    coalesce(sale_qty_budget, 0) as sale_qty_budget,
    coalesce(sale_amt_budget, 0) as sale_amt_budget,
    coalesce(sessions_budget, 0) as sessions_budget,
    coalesce(net_sale_amt_budget, 0) as net_sale_amt_budget,
    coalesce(net_sale_cost_budget, 0) as net_sale_cost_budget,
    coalesce(shipping_budget, 0) as shipping_budget
from cte_calendar
natural left join cte_demand_sales
natural left join cte_net_sales
natural left join cte_sessions
natural left join cte_forecast
where
    coalesce(orders, sessions, net_sale_amt, orders_forecast, orders_budget) is not null