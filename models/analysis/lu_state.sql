{{
    config(
        materialized = 'table',
        pk = ['state_code']
    )
}}
select
    state_code,
    state_name,
    subdivision_category,
    current_timestamp() as inserted_ts
from {{ref('state_code')}}
order by state_code