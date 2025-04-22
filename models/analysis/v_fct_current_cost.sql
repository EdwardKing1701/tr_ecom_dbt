{{
    config(
        materialized = 'view'
    )
}}
select
    itm_key,
    stdcost as current_cost
from {{source('robling_dwh', 'dwh_f_curr_cst_itm_b')}}
where
    cntry_code = 'USA'