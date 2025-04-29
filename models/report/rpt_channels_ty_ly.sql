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
    from {{ref('rpt_channels')}}
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
    sum(iff(time_period = 'TY', sale_amt, 0)) as sale_amt_ty,
    sum(iff(time_period = 'LY', sale_amt, 0)) as sale_amt_ly,
    sum(iff(time_period = 'TY', shipping, 0)) as shipping_ty,
    sum(iff(time_period = 'LY', shipping, 0)) as shipping_ly,
    sum(iff(time_period = 'TY', tax, 0)) as tax_ty,
    sum(iff(time_period = 'LY', tax, 0)) as tax_ly,
    sum(iff(time_period = 'TY', sessions, 0)) as sessions_ty,
    sum(iff(time_period = 'LY', sessions, 0)) as sessions_ly,
    sum(iff(time_period = 'TY', engaged_sessions, 0)) as engaged_sessions_ty,
    sum(iff(time_period = 'LY', engaged_sessions, 0)) as engaged_sessions_ly,
    sum(iff(time_period = 'TY', orders_unadjusted, 0)) as orders_unadjusted_ty,
    sum(iff(time_period = 'LY', orders_unadjusted, 0)) as orders_unadjusted_ly,
    sum(iff(time_period = 'TY', sale_qty_unadjusted, 0)) as sale_qty_unadjusted_ty,
    sum(iff(time_period = 'LY', sale_qty_unadjusted, 0)) as sale_qty_unadjusted_ly,
    sum(iff(time_period = 'TY', sale_amt_unadjusted, 0)) as sale_amt_unadjusted_ty,
    sum(iff(time_period = 'LY', sale_amt_unadjusted, 0)) as sale_amt_unadjusted_ly,
    sum(iff(time_period = 'TY', shipping_unadjusted, 0)) as shipping_unadjusted_ty,
    sum(iff(time_period = 'LY', shipping_unadjusted, 0)) as shipping_unadjusted_ly,
    sum(iff(time_period = 'TY', tax_unadjusted, 0)) as tax_unadjusted_ty,
    sum(iff(time_period = 'LY', tax_unadjusted, 0)) as tax_unadjusted_ly,
    sum(iff(time_period = 'TY', sessions_unadjusted, 0)) as sessions_unadjusted_ty,
    sum(iff(time_period = 'LY', sessions_unadjusted, 0)) as sessions_unadjusted_ly,
    sum(iff(time_period = 'TY', engaged_sessions_unadjusted, 0)) as engaged_sessions_unadjusted_ty,
    sum(iff(time_period = 'LY', engaged_sessions_unadjusted, 0)) as engaged_sessions_unadjusted_ly,
    sum(iff(time_period = 'TY', spend, 0)) as spend_ty,
    sum(iff(time_period = 'LY', spend, 0)) as spend_ly,
    sum(iff(time_period = 'TY', sale_amt_forecast, 0)) as sale_amt_forecast,
    sum(iff(time_period = 'TY', sale_amt_budget, 0)) as sale_amt_budget,
    sum(iff(time_period = 'TY', spend_forecast, 0)) as spend_forecast
from cte_kpi
join cte_xfrm using (xfrm_date)
group by all