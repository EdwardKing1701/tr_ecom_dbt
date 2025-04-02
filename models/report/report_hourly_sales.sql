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
        date_trunc('hour', order_ts) as hour,
        max(iff(order_id like 'FL%', null, order_ts)) as last_order_ts,
        count(distinct order_id) as orders,
        sum(sale_qty) as sale_qty,
        sum(sale_amt) as sale_amt,
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
        date_trunc('hour', order_ts) as hour,
        max(iff(order_id like 'FL%', null, order_ts)) as last_order_ts_api,
        count(distinct order_id) as orders_api,
        sum(quantity) as sale_qty_api,
        sum(price_after_order_discount) as sale_amt_api
    from {{source('sfcc', 'order_product_item')}}
    join cte_orders using(order_id)
    group by all
)
select
    date,
    hour,
    coalesce(last_order_ts_api, last_order_ts) as last_order_ts,
    orders,
    sale_qty,
    sale_amt,
    orders_api,
    sale_qty_api,
    sale_amt_api
from cte_sales_robling
full join cte_sales_api using(date, hour)