{{
    config(
        materialized = 'view'
    )
}}
with
cte_orders as (
    select
        id as order_id,
        case
            when id like 'FL%' then
                to_timestamp_tz(creation_date::timestamp_ntz)
            else
                convert_timezone('America/Los_Angeles', creation_date)
        end as order_ts,
        order_ts::date as order_date
from {{source('sfcc', 'orders_history')}}
where
    order_date = current_date()
    and order_ts <= current_timestamp()
qualify row_number() over (partition by id order by _fivetran_synced desc) = 1
),
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
        order_date as date,
        hour(order_ts) as hour,
        max(iff(order_id like 'FL%', null, order_ts)) as last_order_ts_api,
        count(distinct order_id) as orders_api,
        sum(quantity) as sale_qty_api,
        sum(price_after_order_discount) as sale_amt_api
    from {{source('sfcc', 'order_product_item')}}
    join cte_orders using(order_id)
    group by all
),
cte_sessions as (
    select
        date,
        hour,
        sessions
    from {{ref('ga_hourly')}}
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
        sessions_forecast
    from {{ref('rpt_hourly_sales_forecast')}}
    where
        date between current_date() - 2 and current_date()
)
select
    date,
    hour,
    case
        when orders_api is not null then 'api'
        when orders_robling is not null then 'robling'
    end as data_source,
    coalesce(last_order_ts_api, last_order_ts) as last_order_ts,
    coalesce(orders_api, orders_robling) as orders,
    coalesce(sale_qty_api, sale_qty_robling) as sale_qty,
    coalesce(sale_amt_api, sale_amt_robling) as sale_amt,
    sessions,
    orders_forecast,
    sale_qty_forecast,
    sale_amt_forecast,
    sessions_forecast,
    case
        when data_source = 'robling' and orders > 2 then true
        when minute(last_order_ts) >= 55 then true
        when lead(orders, 1) over (partition by date order by hour) > 2 then true
        else false
    end as is_complete_hour
from cte_sales_robling
full join cte_sales_api using(date, hour)
full join cte_sessions using(date, hour)
full join cte_forecast using(date, hour)