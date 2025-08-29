{{
    config(
        materialized = 'table',
        pk = ['date', 'platform_group', 'device_type']
    )
}}
with
cte_sessions as (
    select
        date,
        platform,
        device_category,
        sessions,
        revenue,
        purchases,
        engaged_sessions
    from {{ref('stg_ga_device_platform')}}
),
cte_items as (
    select
        date,
        platform,
        device_category,
        quantity
    from {{ref('stg_ga_device_platform_items')}}
),
cte_all_ga as (
    select
        date,
        case
            when platform in ('iOS', 'Android') then 'App'
            else 'Web'
        end as platform_group,
        case
            when platform in ('iOS', 'Android') then 'Mobile App'
            when platform = 'web' and device_category = 'desktop' then 'Desktop Web'
            when platform = 'web' then 'Mobile Web'
        end as device_type,
        sum(sessions) as sessions_unadjusted,
        sum(revenue) as sale_amt_unadjusted,
        sum(purchases) as orders_unadjusted,
        sum(engaged_sessions) as engaged_sessions_unadjusted,
        sum(quantity) as sale_qty_unadjusted
    from cte_sessions
    full join cte_items using (date, platform, device_category)
    group by all
),
cte_sales as (
    select
        date,
        case
            when order_type = 'App' then order_type
            when order_type in ('Domestic', 'International') then 'Web'
        end as platform_group,
        count(distinct order_id) as orders_total,
        sum(sale_qty) as sale_qty_total,
        sum(sale_amt) as sale_amt_total
    from {{ref('v_fct_orders')}}
    where
        order_type <> 'Facebook'
    group by all
),
cte_analytics_session as (
    select
        date,
        coalesce(sessions, 0) as sessions_total,
        coalesce(engaged_sessions, 0) as engaged_sessions_total
    from {{ref('stg_ga_sessions')}}
),
cte_adjusted_demand as (
    select
        date,
        platform_group,
        device_type,
        sessions_total * ratio_to_report(sessions_unadjusted) over (partition by date) as sessions,
        engaged_sessions_total * ratio_to_report(engaged_sessions_unadjusted) over (partition by date) as engaged_sessions,
        orders_total * ratio_to_report(orders_unadjusted) over (partition by date, platform_group) as orders,
        sale_qty_total * ratio_to_report(sale_qty_unadjusted) over (partition by date, platform_group) as sale_qty,
        sale_amt_total * ratio_to_report(sale_amt_unadjusted) over (partition by date, platform_group) as sale_amt,
        sessions_unadjusted,
        engaged_sessions_unadjusted,
        orders_unadjusted,
        sale_qty_unadjusted,
        sale_amt_unadjusted
    from cte_all_ga
    natural join cte_sales
    natural join cte_analytics_session   
)
select
    date,
    platform_group,
    device_type,
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
    current_timestamp() as inserted_ts
from cte_adjusted_demand
order by date, platform_group, device_type