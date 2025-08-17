{{
    config(
        materialized = 'view'
    )
}}
with
cte_sessions as (
    select
        date,
        platform,
        sum(sessions) as sessions,
        sum(active_users) as active_users,
        sum(first_opens) as first_opens
    from {{ref('src_ga_app_users')}}
    group by all
),
cte_orders as (
    select
        demand_date as date,
        platform,
        count(distinct order_id) as orders,
        sum(sale_amt) as sale_amt
    from {{ref('stg_sfcc_orders')}}
    where
        platform in ('Android', 'iOS')
        and demand_date between '2023-01-29' and current_date() - 1
    group by all
),
cte_downloads as (
    select
        date,
        platform,
        sum(downloads) as downloads
    from {{ref('fct_app_downloads')}}
    group by all
)
select
    date,
    platform,
    coalesce(sessions, 0) as sessions,
    coalesce(active_users, 0) as active_users,
    coalesce(first_opens, 0) as first_opens,
    coalesce(orders, 0) as orders,
    coalesce(sale_amt, 0) as sale_amt,
    coalesce(downloads, 0) as downloads
from cte_sessions
full join cte_orders using (date, platform)
full join cte_downloads using (date, platform)