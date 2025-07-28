{{
    config(
        materialized = 'table',
        pk = ['itm_key']
    )
}}
select
    itm_key,
    stdcost as current_cost,
    current_timestamp() as inserted_ts
from {{source('robling_dwh', 'dwh_f_curr_cst_itm_b')}}
where
    cntry_code = 'USA'