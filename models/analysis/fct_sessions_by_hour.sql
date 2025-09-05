{{
    config(
        materialized = 'table'
    )
}}
with
cte_analytics_channel as (
    select
        date,
        channel,
        sum(sessions) as sessions_unadjusted,
        sum(engaged_sessions) as engaged_sessions_unadjusted,
        sum(purchases) as orders_unadjusted,
        sum(coalesce(quantity, 0)) as sale_qty_unadjusted,
        sum(revenue) as sale_amt_unadjusted
    from {{ref('stg_ga_channels')}}
    full join {{ref('stg_ga_items')}} using (date, channel_original, channel)
    where
        channel <> 'Unassigned'
        and date >= '2024-02-04'
    group by all
),
cte_analytics_session as (
    select
        date,
        coalesce(sessions, 0) as sessions_total,
        coalesce(engaged_sessions, 0) as engaged_sessions_total
    from {{ref('stg_ga_sessions')}}
    where
        date >= '2024-02-04'
),
cte_demand_sales as (
    select
        date,
        count(distinct order_id) as orders_total,
        sum(sale_qty) as sale_qty_total,
        sum(sale_amt) as sale_amt_total
    from {{ref('v_fct_orders')}}
    where
        date >= '2024-02-04'
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
        sessions_unadjusted,
        engaged_sessions_unadjusted,
        orders_unadjusted,
        sale_qty_unadjusted,
        sale_amt_unadjusted
    from cte_analytics_channel
    natural join cte_demand_sales
    natural join cte_analytics_session
),
cte_hour_setup as (
    select
        row_number() over (order by 1) - 1 as hour
    from table(generator(rowcount=>24))
),
cte_hourly as (
    select
        date,
        channel,
        hour,
        sum(coalesce(sessions, 0)) as hourly_sessions,
        sum(coalesce(engaged_sessions, 0)) as hourly_engaged_sessions,
        sum(coalesce(transactions, 0)) as hourly_orders,
        sum(coalesce(quantity, 0)) as hourly_sale_qty,
        sum(coalesce(purchase_revenue, 0)) as hourly_sale_amt
    from {{ref('stg_ga_hourly_channels')}}
    full join {{ref('stg_ga_hourly_channels_items')}} using (date, channel_original, channel, hour)
    where
        channel <> 'Unassigned'
    group by all
),
cte_hourly_ratio as (
    select
        date,
        channel,
        hour,
        sessions,
        engaged_sessions,
        orders,
        sale_qty,
        sale_amt,
        sessions_unadjusted,
        engaged_sessions_unadjusted,
        orders_unadjusted,
        sale_qty_unadjusted,
        sale_amt_unadjusted,
        ratio_to_report(coalesce(hourly_sessions, 0)) over (partition by date, channel) as hourly_sessions_ratio,
        ratio_to_report(coalesce(hourly_engaged_sessions, 0)) over (partition by date, channel) as hourly_engaged_sessions_ratio,
        ratio_to_report(coalesce(hourly_orders, 0)) over (partition by date, channel) as hourly_orders_ratio,
        ratio_to_report(coalesce(hourly_sale_qty, 0)) over (partition by date, channel) as hourly_sale_qty_ratio,
        ratio_to_report(coalesce(hourly_sale_amt, 0)) over (partition by date, channel) as hourly_sale_amt_ratio
    from cte_adjusted_demand
    join cte_hour_setup
    left join cte_hourly using (date, channel, hour)
)
select
    date,
    hour,
    channel,
    coalesce(sessions, 0) * coalesce(hourly_sessions_ratio, 0) as sessions,
    coalesce(engaged_sessions, 0) * coalesce(hourly_engaged_sessions_ratio, 0) as engaged_sessions,
    coalesce(orders, 0) * coalesce(hourly_orders_ratio, 0) as orders,
    coalesce(sale_qty, 0) * coalesce(hourly_sale_qty_ratio, 0) as sale_qty,
    coalesce(sale_amt, 0) * coalesce(hourly_sale_amt_ratio, 0) as sale_amt,
    coalesce(sessions_unadjusted, 0) * coalesce(hourly_sessions_ratio, 0) as sessions_unadjusted,
    coalesce(engaged_sessions_unadjusted, 0) * coalesce(hourly_engaged_sessions_ratio, 0) as engaged_sessions_unadjusted,
    coalesce(orders_unadjusted, 0) * coalesce(hourly_orders_ratio, 0) as orders_unadjusted,
    coalesce(sale_qty_unadjusted, 0) * coalesce(hourly_sale_qty_ratio, 0) as sale_qty_unadjusted,
    coalesce(sale_amt_unadjusted, 0) * coalesce(hourly_sale_amt_ratio, 0) as sale_amt_unadjusted,
    current_timestamp() as inserted_ts
from cte_hourly_ratio
order by date, hour, channel