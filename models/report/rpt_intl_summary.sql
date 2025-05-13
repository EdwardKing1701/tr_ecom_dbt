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
        order_ts::date as order_date,
        lower(coalesce(c_flow_experience_id, c_ge_customer_shipping_country_name)) as region
from {{source('sfcc', 'orders_history')}}
where
    order_id like 'FL%'
    and lower(coalesce(c_flow_experience_id, c_ge_customer_shipping_country_name)) is not null
qualify row_number() over (partition by id order by _fivetran_synced desc) = 1
),
cte_sales as (
    select
        date,
        count(distinct order_id) as orders,
        sum(sale_qty) as sale_qty,
        sum(sale_amt) as sale_amt,
        sum(sale_amt) - sum(sale_cost) as gross_margin
    from {{ref('v_fct_orders')}}
    join cte_orders using (order_id)
    group by all
),
cte_traffic as (
    select
        date,
        sessions
    from {{ref('ga_international')}}
)
select
    *
from cte_sales
natural full join cte_traffic
where
    date < current_date()