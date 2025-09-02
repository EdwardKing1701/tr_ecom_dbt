{{
    config(
        materialized = 'table',
        pk = ['itm_key']
    )
}}
select
    itm_key,
    cost_current as current_cost,
    current_timestamp() as inserted_ts
from {{ref('stg_robling_item_cost_current')}}