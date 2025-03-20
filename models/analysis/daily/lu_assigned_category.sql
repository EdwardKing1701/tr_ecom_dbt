{{
    config(
        materialized = 'table',
        pk = ['style', 'category_id']
    )
}}
with
cte_master as (
    select
        id as product_id
    from {{source('sfcc', 'product')}}
    where
        type_master
)
select
    product_id as style,
    category_id,
    convert_timezone('America/Los_Angeles', _fivetran_synced) as source_synced_ts,
    current_timestamp() as inserted_ts
from {{source('sfcc', 'assigned_category')}}
natural join cte_master
where
    catalog_id = 'tr-ecom-catalog'
order by style, category_id