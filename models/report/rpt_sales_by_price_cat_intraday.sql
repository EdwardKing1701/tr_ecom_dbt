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
    from {{ref('dim_item')}}
),
cte_price as (
    select
        date,
        color,
        price_list,
        price_category
    from {{ref('lu_price_list_history_by_day')}}
    where
    (
        date between current_date() - 1 and current_date()
        or date in (select date_ly from {{ref('dim_date')}} where date between current_date() - 1 and current_date())
    )
),
cte_sales as (
    select
        itm_key,
        demand_date as date,
        hour(demand_ts) as hour,
        sale_qty,
        sale_cost,
        sale_amt
    from {{ref('v_sfcc_orders')}}
    where
    (
        demand_date between current_date() - 1 and current_date()
        or demand_date in (select date_ly from {{ref('dim_date')}} where date between current_date() - 1 and current_date())
    )
    and demand_ts <= current_timestamp()
)
select
    date,
    hour,
    division,
    class,
    price_list,
    coalesce(price_category, 'REG') as price_category,
    sum(sale_qty) as sale_qty,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt
from cte_sales
left join cte_catalogue using (itm_key)
left join cte_price using (date, color)
group by all