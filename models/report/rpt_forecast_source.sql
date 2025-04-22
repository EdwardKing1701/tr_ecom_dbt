{{
    config(
        materialized = 'view'
    )
}}
with
cte_forecast as (
    select
        date,
        month_id,
        orders_forecast,
        ratio_to_report(orders_forecast) over (partition by month_id) as share_of_monthly_orders_forecast,
        units_forecast as sale_qty_forecast,
        ratio_to_report(sale_qty_forecast) over (partition by month_id) as share_of_monthly_sale_qty_forecast,
        demand_forecast as sale_amt_forecast,
        ratio_to_report(sale_amt_forecast) over (partition by month_id) as share_of_monthly_sale_amt_forecast,
        traffic_forecast as sessions_forecast,
        ratio_to_report(sessions_forecast) over (partition by month_id) as share_of_monthly_sessions_forecast
    from {{ref('ecom_demand_plan')}}
    join {{ref('dim_date')}} using (date)
),
cte_budget_monthly as (
    select
        month_id,
        orders_budget,
        coalesce(sale_qty_budget, upt_forecast * orders_budget) as sale_qty_budget,
        sale_amt_budget,
        sessions_budget
    from {{ref('ecom_demand_budget')}}
    left join (
        select
            month_id,
            sum(sale_qty_forecast) / nullifzero(sum(orders_forecast)) as upt_forecast
        from cte_forecast
        join {{ref('dim_date')}} using (date)
        group by all
    ) using (month_id)
),
cte_budget_daily as (
    select
        date,
        month_id,
        orders_budget * share_of_monthly_orders_forecast as orders_budget,
        sale_qty_budget * share_of_monthly_sale_qty_forecast as sale_qty_budget,
        sale_amt_budget * share_of_monthly_sale_amt_forecast as sale_amt_budget,
        sessions_budget * share_of_monthly_sessions_forecast as sessions_budget
    from cte_budget_monthly
    join cte_forecast using (month_id)
),
cte_channel_forecast as (
    select
        date,
        channel,
        sale_amt_forecast,
        ratio_to_report(sale_amt_forecast) over (partition by date) as share_of_daily_sale_amt_forecast,
        sale_amt_budget * share_of_daily_sale_amt_forecast as sale_amt_budget
    from {{ref('ecom_channel_plan')}}
    left join cte_budget_daily using (date)
),
cte_all_forecast as (
    select
        date,
        channel,
        null as orders_forecast,
        null as sale_qty_forecast,
        sale_amt_forecast,
        null as sessions_forecast,
        null as orders_budget,
        null as sale_qty_budget,
        sale_amt_budget,
        null as sessions_budget
    from cte_channel_forecast

    union all

    select
        date,
        'ECOM Total' as channel,
        orders_forecast,
        sale_qty_forecast,
        sale_amt_forecast,
        sessions_forecast,
        orders_budget,
        sale_qty_budget,
        sale_amt_budget,
        sessions_budget
    from cte_forecast
    left join cte_budget_daily using (date)
)
select *
from cte_all_forecast