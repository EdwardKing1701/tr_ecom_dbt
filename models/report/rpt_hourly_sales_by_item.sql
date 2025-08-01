{{
    config(
        materialized = 'view'
    )
}}
with
cte_items as (
    select
        sku,
        size,
        color,
        color_desc,
        style,
        style_desc,
        division,
        class,
        null as is_collection,
        null as collection_name
    from {{ref('dim_item')}}
),
cte_inventory as (
    select
        product_id as sku,
        allocation_amount,
        ats,
        stock_level
    from {{source('sfcc', 'inventory_list_record')}}
    where
        inventory_list_id = 'dfs-inv-list'
        and not _fivetran_deleted
),
cte_sales_by_item as (
    select
        sku,
        sum(sale_qty) as sale_qty,
        sum(sale_amt) as sale_amt
    from {{ref('stg_sfcc_orders')}}
    where
        demand_date = current_date()
        and demand_ts <= current_timestamp()
    group by all
)
select
    sku,
    color,
    style,
    style_desc,
    color_desc,
    size,
    division,
    class,
    is_collection,
    collection_name,
    coalesce(allocation_amount, 0) as allocation_amount,
    coalesce(ats, 0) as ats,
    coalesce(stock_level, 0) as stock_level,
    coalesce(sale_qty, 0) as sale_qty,
    coalesce(sale_amt, 0) as sale_amt
from cte_sales_by_item
natural full join cte_inventory
natural join cte_items