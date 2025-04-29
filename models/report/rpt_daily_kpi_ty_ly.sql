{{
    config(
        materialized = 'view'
    )
}}
with
cte_kpi as (
    select
        date as xfrm_date,
        * exclude (date)
    from {{ref('rpt_daily_kpi')}}
),
cte_xfrm as (
    select
        xfrm_date,
        date,
        time_period
    from {{ref('date_xfrm')}}
    where
        to_date_type = 'TODAY'
        and time_period in ('TY', 'LY')
)
select
    date,
    sum(iff(time_period = 'TY', orders, 0)) as orders_ty,
    sum(iff(time_period = 'LY', orders, 0)) as orders_ly,
    sum(iff(time_period = 'TY', sale_qty, 0)) as sale_qty_ty,
    sum(iff(time_period = 'LY', sale_qty, 0)) as sale_qty_ly,
    sum(iff(time_period = 'TY', sale_cost, 0)) as sale_cost_ty,
    sum(iff(time_period = 'LY', sale_cost, 0)) as sale_cost_ly,
    sum(iff(time_period = 'TY', sale_amt, 0)) as sale_amt_ty,
    sum(iff(time_period = 'LY', sale_amt, 0)) as sale_amt_ly,
    sum(iff(time_period = 'TY', shipping, 0)) as shipping_ty,
    sum(iff(time_period = 'LY', shipping, 0)) as shipping_ly,
    sum(iff(time_period = 'TY', tax, 0)) as tax_ty,
    sum(iff(time_period = 'LY', tax, 0)) as tax_ly,
    sum(iff(time_period = 'TY', net_sale_qty, 0)) as net_sale_qty_ty,
    sum(iff(time_period = 'LY', net_sale_qty, 0)) as net_sale_qty_ly,
    sum(iff(time_period = 'TY', net_sale_cost, 0)) as net_sale_cost_ty,
    sum(iff(time_period = 'LY', net_sale_cost, 0)) as net_sale_cost_ly,
    sum(iff(time_period = 'TY', net_sale_amt, 0)) as net_sale_amt_ty,
    sum(iff(time_period = 'LY', net_sale_amt, 0)) as net_sale_amt_ly,
    sum(iff(time_period = 'TY', gross_margin, 0)) as gross_margin_ty,
    sum(iff(time_period = 'LY', gross_margin, 0)) as gross_margin_ly,
    sum(iff(time_period = 'TY', gross_sale_qty, 0)) as gross_sale_qty_ty,
    sum(iff(time_period = 'LY', gross_sale_qty, 0)) as gross_sale_qty_ly,
    sum(iff(time_period = 'TY', gross_sale_cost, 0)) as gross_sale_cost_ty,
    sum(iff(time_period = 'LY', gross_sale_cost, 0)) as gross_sale_cost_ly,
    sum(iff(time_period = 'TY', gross_sale_amt, 0)) as gross_sale_amt_ty,
    sum(iff(time_period = 'LY', gross_sale_amt, 0)) as gross_sale_amt_ly,
    sum(iff(time_period = 'TY', return_qty, 0)) as return_qty_ty,
    sum(iff(time_period = 'LY', return_qty, 0)) as return_qty_ly,
    sum(iff(time_period = 'TY', return_cost, 0)) as return_cost_ty,
    sum(iff(time_period = 'LY', return_cost, 0)) as return_cost_ly,
    sum(iff(time_period = 'TY', return_amt, 0)) as return_amt_ty,
    sum(iff(time_period = 'LY', return_amt, 0)) as return_amt_ly,
    sum(iff(time_period = 'TY', sessions, 0)) as sessions_ty,
    sum(iff(time_period = 'LY', sessions, 0)) as sessions_ly,
    sum(iff(time_period = 'TY', engaged_sessions, 0)) as engaged_sessions_ty,
    sum(iff(time_period = 'LY', engaged_sessions, 0)) as engaged_sessions_ly,
    sum(iff(time_period = 'TY', orders_forecast, 0)) as orders_forecast,
    sum(iff(time_period = 'TY', sale_qty_forecast, 0)) as sale_qty_forecast,
    sum(iff(time_period = 'TY', sale_amt_forecast, 0)) as sale_amt_forecast,
    sum(iff(time_period = 'TY', sessions_forecast, 0)) as sessions_forecast,
    sum(iff(time_period = 'TY', orders_budget, 0)) as orders_budget,
    sum(iff(time_period = 'TY', sale_qty_budget, 0)) as sale_qty_budget,
    sum(iff(time_period = 'TY', sale_amt_budget, 0)) as sale_amt_budget,
    sum(iff(time_period = 'TY', sessions_budget, 0)) as sessions_budget
from cte_kpi
join cte_xfrm using (xfrm_date)
group by all