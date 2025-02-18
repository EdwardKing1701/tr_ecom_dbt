{{
    config(
        materialized = 'view'
    )
}}
select distinct
    sty_id,
    sty_desc,
    rep_color_id,
    rep_color_desc,
    sbc_desc,
    cls_desc,
    dpt_desc,
    div_desc
from {{ref('rpt_best_sellers_by_channel')}}