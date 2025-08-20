{{
    config(
        materialized = 'view'
    )
}}
with
cte_sales_robling as (
    select
        date,
        hour(order_ts) as hour,
        max(iff(order_id like 'FL%', null, order_ts)) as last_order_ts,
        count(distinct order_id) as orders_robling,
        sum(sale_qty) as sale_qty_robling,
        sum(sale_amt) as sale_amt_robling,
        current_timestamp() as inserted_ts
    from {{ref('v_fct_order_items')}}
    where
        (
            date between current_date() - 2 and current_date()
            or date in (select date_ly from {{ref('dim_date')}} where date between current_date() - 2 and current_date())
        )
    group by all
),
cte_sales_api as (
    select
        demand_date as date,
        hour(demand_ts) as hour,
        max(iff(order_type = 'International', null, demand_ts)) as last_order_ts_api,
        count(distinct order_id) as orders_api,
        sum(sale_qty) as sale_qty_api,
        sum(sale_amt) as sale_amt_api
    from {{ref('v_sfcc_orders')}}
    where
        demand_date = current_date()
        and demand_ts <= current_timestamp()
    group by all
),
cte_sessions as (
    select
        date,
        hour,
        sessions
    from {{ref('src_ga_hourly')}}
    where
        (
            date between current_date() - 2 and current_date()
            or date in (select date_ly from {{ref('dim_date')}} where date between current_date() - 2 and current_date())
        )
),
cte_forecast as (
    select
        date,
        hour,
        orders_forecast,
        sale_qty_forecast,
        sale_amt_forecast,
        sessions_forecast,
        orders_budget,
        sale_qty_budget,
        sale_amt_budget,
        sessions_budget
    from {{ref('rpt_hourly_sales_forecast')}}
    where
        date between current_date() - 2 and current_date()
)
select
    date,
    hour,
    case
        when orders_api is not null and coalesce(orders_api, 0) >= (coalesce(orders_robling, 0) * 0.75) then 'api'
        when orders_robling is not null then 'robling'
    end as data_source,
    coalesce(last_order_ts_api, last_order_ts) as last_order_ts,
    iff(data_source = 'api', orders_api, orders_robling) as orders,
    iff(data_source = 'api', sale_qty_api, sale_qty_robling) as sale_qty,
    iff(data_source = 'api', sale_amt_api, sale_amt_robling) as sale_amt,
    sessions,
    orders_forecast,
    sale_qty_forecast,
    sale_amt_forecast,
    sessions_forecast,
    orders_budget,
    sale_qty_budget,
    sale_amt_budget,
    sessions_budget,
    case
        when data_source = 'robling' and orders > 2 then true
        when minute(last_order_ts) >= 55 then true
        when lead(orders, 1) over (partition by date order by hour) > 2 then true
        else false
    end as is_complete_hour
from cte_sales_robling
full join cte_sales_api using (date, hour)
full join cte_sessions using (date, hour)
full join cte_forecast using (date, hour)