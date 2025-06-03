{{
    config(
        materialized = 'view'
    )
}}
with
cte_catalogue as (
    select
        itm_key,
        color,
        division,
        department,
        class
    from {{ref('dim_item')}}
),
cte_price_history as (
    select
        date as order_date,
        color,
        price_category
    from {{ref('v_price_list_history_by_day')}}
),
cte_sales as (
    select
        date as order_date,
        itm_key,
        sale_qty,
        sale_cost,
        sale_amt
    from {{ref('v_fct_order_items')}}
),
cte_calendar as (
    select
        date,
        xfrm_date as order_date,
        time_period,
        week_id,
        month_id
    from {{ref('date_xfrm')}}
    join {{ref('dim_date')}} using (date)
    where
        to_date_type = 'TODAY'
        and time_period in ('TY', 'LY')
        and year_id >= (select year_id - 2 from {{ref('dim_date')}} where date = previous_day(current_date(), 'sa'))
        and date <= previous_day(current_date(), 'sa')
)
select
    week_id,
    month_id,
    time_period,
    division,
    department,
    class,
    coalesce(price_category, 'REG') as price_category,
    sum(sale_qty) as sale_qty,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt
from cte_sales
join cte_catalogue using (itm_key)
left join cte_price_history using (order_date, color)
join cte_calendar using (order_date)
group by all