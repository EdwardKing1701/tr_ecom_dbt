{{
    config(
        materialized = 'table',
        pk = ['date', 'time_period']
    )
}}
with
cte_calendar as (
    select
        month_start_date as date,
        month_id
    from {{ref('dim_date')}}
    where
        day_in_month = 1
),
cte_budget as (
    select
        month_id,
        'TY' as time_period,
        base_sales as base_sales_budget,
        returns as returns_budget,
        discounts_and_allowance as discounts_and_allowance_budget,
        shipping_revenue as shipping_revenue_budget,
        net_sales as net_sales_budget,
        cogs as cogs_budget
    from {{source('load', 'planful_net_sales')}}
    where
        dimension = 'Budget'
),
cte_actual as (
    select
        month_id,
        dimension as time_period,
        base_sales,
        returns,
        discounts_and_allowance,
        shipping_revenue,
        net_sales,
        cogs
    from {{source('load', 'planful_net_sales')}}
    where
        dimension in ('TY', 'LY')
)
select
    date,
    time_period,
    coalesce(base_sales, 0) as base_sales,
    coalesce(returns, 0) as returns,
    coalesce(discounts_and_allowance, 0) as discounts_and_allowance,
    coalesce(shipping_revenue, 0) as shipping_revenue,
    coalesce(net_sales, 0) as net_sales,
    coalesce(cogs, 0) as cogs,
    coalesce(base_sales_budget, 0) as base_sales_budget,
    coalesce(returns_budget, 0) as returns_budget,
    coalesce(discounts_and_allowance_budget, 0) as discounts_and_allowance_budget,
    coalesce(shipping_revenue_budget, 0) as shipping_revenue_budget,
    coalesce(net_sales_budget, 0) as net_sales_budget,
    coalesce(cogs_budget, 0) as cogs_budget
from cte_actual
full join cte_budget using (month_id, time_period)
join cte_calendar using (month_id)
order by date, time_period