{{
    config(
        materialized = 'table',
        pk = ['itm_key']
    )
}}
select
    itm_key,
    sku,
    color,
    style,
    color_id,
    color_desc,
    size_id,
    size,
    style_desc,
    division_id,
    division,
    department_id,
    department,
    class_id,
    class,
    subclass_id,
    subclass,
    color_sku_index,
    style_sku_index,
    current_timestamp() as inserted_ts
from {{ref('stg_items')}}
order by itm_key