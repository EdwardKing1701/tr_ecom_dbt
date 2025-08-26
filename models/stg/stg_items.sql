{{
    config(
        materialized = 'table'
    )
}}
with
cte_robling as (
    select
        *
    from {{source('robling_merch', 'dv_dwh_d_prd_itm_lu')}}
    where
        itm_key <> -1
)
select
    itm_key as itm_key,
    coalesce(itm_id, '(N/A)') as sku,
    coalesce(sty_id || '-' || color_num, '(N/A)') as color,
    coalesce(sty_id, '(N/A)') as style,
    coalesce(color_num, '(N/A)') as color_id,
    coalesce(color_desc, '(N/A)') as color_desc,
    coalesce(size_id, '(N/A)') as size_id,
    coalesce(size_desc, '(N/A)') as size,
    coalesce(sty_desc, '(N/A)') as style_desc,
    coalesce(div_id, '(N/A)') as division_id,
    coalesce(div_desc, '(N/A)') as division,
    coalesce(dpt_id, '(N/A)') as department_id,
    coalesce(dpt_desc, '(N/A)') as department,
    coalesce(cls_id, '(N/A)') as class_id,
    coalesce(cls_desc, '(N/A)') as class,
    coalesce(sbc_id, '(N/A)') as subclass_id,
    coalesce(sbc_desc, '(N/A)') as subclass,
    row_number() over (partition by color order by sku) as color_sku_index,
    row_number() over (partition by style order by sku) as style_sku_index
from cte_robling
full join (select -1 as itm_key) using (itm_key)