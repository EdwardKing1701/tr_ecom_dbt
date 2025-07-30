{{
    config(
        materialized = 'table',
        pk = ['country_code']
    )
}}
select
    country_code,
    country_name,
    current_timestamp() as inserted_ts
from {{ref('country_code')}}
order by country_code