{{
    config(
        materialized = 'view'
    )
}}
select
    itm_key,
    sty_clr_code as color,
    day_key as date,
    stdcost as cost_historical
from {{source('robling_dwh', 'dwh_f_cst_itm_b')}}
where
    cntry_code = 'USA'