{{
    config(
        materialized = 'table',
        pk = ['date', 'platform']
    )
}}
select
    date,
    lower(platform) as platform,
    users,
    current_timestamp() as inserted_ts
from {{ref('ga_add_to_cart_users')}}
order by date, platform