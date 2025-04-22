{{
    config(
        materialized = 'view'
    )
}}
with
cte_catalogue as (
    select
        itm_key,
        sku as product_id,
        color,
        division,
        class
    from {{ref('v_dim_item')}}
),
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
    (
        order_date between current_date() - 1 and current_date()
        or order_date in (select date_ly from {{ref('dim_date')}} where date between current_date() - 1 and current_date())
    )
    and order_ts <= current_timestamp()
qualify row_number() over (partition by id order by _fivetran_synced desc) = 1
)
select
    order_date as date,
    hour(order_ts) as hour,
    division,
    class,
    price_list,
    coalesce(price_category, 'REG') as price_category,
    sum(quantity) as sale_qty,
    sum(quantity * current_cost) as sale_cost,
    sum(price_after_order_discount) as sale_amt
from {{source('sfcc', 'order_product_item')}}
join cte_orders using (order_id)
left join cte_catalogue using (product_id)
left join {{ref('lu_price_list_history')}} using (color)
left join {{ref('v_fct_current_cost')}} using(itm_key)
where
    coalesce(effective_date, '1901-01-01') <= order_date
    and coalesce(end_date, '2199-01-01') >= order_date
group by all