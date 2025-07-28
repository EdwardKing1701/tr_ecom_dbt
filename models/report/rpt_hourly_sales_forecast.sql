{{
    config(
        materialized = 'view'
    )
}}
with
cte_report_date as (
    select
        date as report_date
    from {{ref('dim_date')}}
    where
        date between current_date() - 2 and current_date()
),
cte_comp_dates as (
    -- get date of 5 previous same days of week
    select
        report_date,
        date
    from {{ref('dim_date')}}
    join cte_report_date
    where
        date < report_date
        and dayofweek(date) = dayofweek(report_date)
        and date >= dateadd('day', -35, report_date)
        and not report_date in (select date from {{source('gsheets', 'intraday_comparison_override')}})
),
cte_comp_dates_override as (
    select
        date as report_date,
        comparable_date as date
    from {{source('gsheets', 'intraday_comparison_override')}}
    where
        date in (select report_date from cte_report_date)
    union all
    select
        report_date,
        date
    from cte_comp_dates
),
cte_sales_hourly as (
    select
        report_date,
        date,
        hour(order_ts) as hour,
        count(distinct order_id) as orders_hourly,
        sum(sale_qty) as sale_qty_hourly,
        sum(sale_amt) as sale_amt_hourly
    from {{ref('v_fct_order_items')}}
    join cte_comp_dates_override using (date)
    group by all
),
cte_sessions_hourly as (
    select
        report_date,
        date,
        hour,
        sessions as sessions_hourly
    from {{ref('ga_hourly')}}
    join cte_comp_dates_override using (date)
),
cte_share_of_sales_by_date as (
    select
        *,
        ratio_to_report(orders_hourly) over (partition by date) as share_of_orders,
        ratio_to_report(sale_qty_hourly) over (partition by date) as share_of_sale_qty,
        ratio_to_report(sale_amt_hourly) over (partition by date) as share_of_sale_amt,
        ratio_to_report(sessions_hourly) over (partition by date) as share_of_sessions
    from cte_sales_hourly
    natural full join cte_sessions_hourly
),
cte_avg_share_of_sales as (
    select
        report_date,
        hour,
        avg(share_of_orders) as avg_share_of_orders,
        avg(share_of_sale_qty) as avg_share_of_sale_qty,
        avg(share_of_sale_amt) as avg_share_of_sale_amt,
        avg(share_of_sessions) as avg_share_of_sessions
    from cte_share_of_sales_by_date
    group by all
),
cte_forecast as (
    select
        date as report_date,
        orders_forecast as orders_forecast_daily,
        sale_qty_forecast as sale_qty_forecast_daily,
        sale_amt_forecast as sale_amt_forecast_daily,
        sessions_forecast as sessions_forecast_daily,
        orders_budget as orders_budget_daily,
        sale_qty_budget as sale_qty_budget_daily,
        sale_amt_budget as sale_amt_budget_daily,
        sessions_budget as sessions_budget_daily
    from {{ref('fct_forecast_by_day')}}
)
select
    report_date as date,
    hour,
    avg_share_of_orders * orders_forecast_daily as orders_forecast,
    avg_share_of_sale_qty * sale_qty_forecast_daily as sale_qty_forecast,
    avg_share_of_sale_amt * sale_amt_forecast_daily as sale_amt_forecast,
    avg_share_of_sessions * sessions_forecast_daily as sessions_forecast,
    avg_share_of_orders * orders_budget_daily as orders_budget,
    avg_share_of_sale_qty * sale_qty_budget_daily as sale_qty_budget,
    avg_share_of_sale_amt * sale_amt_budget_daily as sale_amt_budget,
    avg_share_of_sessions * sessions_budget_daily as sessions_budget
from cte_avg_share_of_sales
join cte_forecast using (report_date)