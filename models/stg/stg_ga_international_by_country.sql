{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    country_code,
    sessions,
    purchases,
    revenue,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts
from {{source('load', 'ga_international_by_country')}}