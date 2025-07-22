{{
    config(
        materialized = 'view'
    )
}}
with
cte_sales_setup as (
    select
        date as xfrm_date,
        channel,
        itm_key,
        sale_amt,
        sale_qty,
        sale_cost
    from {{ref('v_fct_order_items')}}
),
cte_sales as (
    select
        date,
        time_period,
        to_date_type,
        division,
        class,
        price_category as price_category_original,
        price_list,
        channel,
        coalesce(price_category, 'REG') as price_category,
        sum(sale_amt) as sale_amt,
        sum(sale_qty) as sale_qty,
        sum(sale_cost) as sale_cost
    from cte_sales_setup
    join {{ref('dim_item')}} using (itm_key)
    join {{ref('date_xfrm')}} using (xfrm_date)
    join {{ref('dim_date')}} using (date)
    left join {{ref('v_price_list_history_by_day')}} using (date, color)
    where
        to_date_type in ('TODAY', 'WTD', 'MTD', 'YTD')
        and time_period in ('TY', 'LY')
    group by all
)
select *
from cte_sales