{{
    config(
        materialized = 'table',
        pk = ['date', 'color']
    )
}}
select
    date,
    color,
    price_list,
    effective_date,
    end_date,
    sale_price,
    price_category,
    promo,
    notes,
    holds,
    current_timestamp() as inserted_ts
from {{ref('dim_date')}}
left join {{ref('lu_price_list_history')}}
    on date >= effective_date
    and date <= end_date
where
    date <= current_date()
    and date >= (select min(effective_date) from {{ref('lu_price_list_history')}})
order by date, color