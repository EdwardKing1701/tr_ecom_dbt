{{
    config(
        materialized = 'view'
    )
}}
select
    itm_key,
    itm_id as sku,
    color_id as color,
    color_desc as color_desc,
    size_id,
    size_desc as size,
    sty_key,
    sty_id as style,
    sty_desc as style_desc,
    div_id as division_id,
    div_desc as division,
    dpt_id as department_id,
    dpt_desc as department,
    cls_id as class_id,
    cls_desc as class,
    sbc_id as subclass_id,
    sbc_desc as subclass
from {{source('robling_merch', 'dv_dwh_d_prd_itm_lu')}}