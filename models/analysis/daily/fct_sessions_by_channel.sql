{{
    config(
        materialized = 'table',
        pk = ['date', 'channel']
    )
}}
with
cte_analytics_channel as (
    select
        date,
        coalesce(channel_correction, channel) as channel,
        sum(sessions) as sessions_unadjusted,
        sum(engaged_sessions) as engaged_sessions_unadjusted,
        sum(purchases) as orders_unadjusted,
        sum(quantity) as sale_qty_unadjusted,
        sum(revenue) as sale_amt_unadjusted,
        sum(shipping) as shipping_unadjusted,
        sum(tax) as tax_unadjusted
    from {{ref('ga_channels')}}
    full join {{ref('ga_items')}} using (date, channel)
    left join {{ref('channel_correction')}} using (channel)
    group by all
),
cte_analytics_session as (
    select
        date,
        coalesce(sessions, 0) as sessions_total,
        coalesce(engaged_sessions, 0) as engaged_sessions_total
    from {{ref('ga_sessions')}}
),
cte_demand_sales as (
    select
        date,
        count(distinct order_id) as orders_total,
        sum(sale_qty) as sale_qty_total,
        sum(sale_amt) as sale_amt_total,
        sum(shipping) as shipping_total,
        sum(tax) as tax_total
    from {{ref('v_fct_orders')}}
    group by all
),
cte_adjusted_demand as (
    select
        date,
        channel,
        sessions_total * ratio_to_report(sessions_unadjusted) over (partition by date) as sessions,
        engaged_sessions_total * ratio_to_report(engaged_sessions_unadjusted) over (partition by date) as engaged_sessions,
        orders_total * ratio_to_report(orders_unadjusted) over (partition by date) as orders,
        sale_qty_total * ratio_to_report(sale_qty_unadjusted) over (partition by date) as sale_qty,
        sale_amt_total * ratio_to_report(sale_amt_unadjusted) over (partition by date) as sale_amt,
        shipping_total * ratio_to_report(shipping_unadjusted) over (partition by date) as shipping,
        tax_total * ratio_to_report(tax_unadjusted) over (partition by date) as tax,
        sessions_unadjusted,
        engaged_sessions_unadjusted,
        orders_unadjusted,
        sale_qty_unadjusted,
        sale_amt_unadjusted,
        shipping_unadjusted,
        tax_unadjusted
    from cte_analytics_channel
    natural join cte_demand_sales
    natural join cte_analytics_session
)
select
    *,
    current_timestamp() as inserted_ts
from cte_adjusted_demand
order by date, channel