{{
    config(
        materialized = 'table',
        pk = ['itm_key']
    )
}}
select
    itm_key,
    itm_id as sku,
    sty_id || '-' || color_num as color,
    sty_id as style,
    color_num as color_id,
    color_desc as color_desc,
    size_id,
    size_desc as size,
    sty_key,
    sty_desc as style_desc,
    div_id as division_id,
    div_desc as division,
    dpt_id as department_id,
    dpt_desc as department,
    cls_id as class_id,
    cls_desc as class,
    sbc_id as subclass_id,
    sbc_desc as subclass,
    row_number() over (partition by color order by sku) as color_sku_index,
    row_number() over (partition by style order by sku) as style_sku_index,
    current_timestamp() as inserted_ts
from {{source('robling_merch', 'dv_dwh_d_prd_itm_lu')}}
order by itm_key