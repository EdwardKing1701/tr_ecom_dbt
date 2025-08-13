{{
    config(
        materialized = 'table',
        pk = ['date']
    )
}}
select
    date,
    windows_signage,
    interiors,
    store_promo_ty,
    ecom_promo_ty,
    store_promo_ly,
    ecom_promo_ly,
    convert_timezone('America/Los_Angeles', inserted_ts) as source_synced_ts,
    current_timestamp() as inserted_ts
from {{source('load', 'promo_calendar')}}
order by date