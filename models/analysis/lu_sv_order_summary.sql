{{
    config(
        materialized = 'table',
        pk = ['created_date', 'shipped_date', 'status']
    )
}}
select
    creation_date as created_date,
    coalesce(ship_date, '2199-01-01') as shipped_date,
    status,
    orders,
    units
from {{ref('sv_order_summary')}}
order by created_date, shipped_date, status