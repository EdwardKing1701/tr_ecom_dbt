{{
    config(
        materialized = 'view',
        pk = ['itm_key']
    )
}}
with
cte_itm_key as (
    select
        itm_key,
        itm_id as sku
from {{source('robling_merch', 'dv_dwh_d_prd_itm_lu')}}
),
cte_sfcc_inventory as (
    select
        product_id::varchar as sku,
        allocation_amount,
        ats,
        stock_level
    from {{source('sfcc', 'inventory_list_record')}}
    where
        inventory_list_id = 'dfs-inv-list'
        and not _fivetran_deleted
)
select
    itm_key,
    allocation_amount,
    ats,
    stock_level
from cte_sfcc_inventory
join cte_itm_key using (sku)
order by itm_key