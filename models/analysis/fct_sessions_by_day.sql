{{
    config(
        materialized = 'table',
        pk = ['date']
    )
}}
select
    date,
    coalesce(sessions, 0) as sessions,
    coalesce(engaged_sessions, 0) as engaged_sessions,
    current_timestamp() as inserted_ts
from {{ref('stg_ga_sessions')}}
order by date