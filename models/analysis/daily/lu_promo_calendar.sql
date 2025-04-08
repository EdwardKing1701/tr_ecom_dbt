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
    current_timestamp() as inserted_ts
from {{ref('promo_calendar')}}
order by date