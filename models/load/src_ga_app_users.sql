{{
    config(
        materialized = 'view'
    )
}}
select
    date,
    platform,
    sessions,
    active_users,
    first_opens
from {{ref('ga_app_users')}}