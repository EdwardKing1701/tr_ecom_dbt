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
        week_id,
        date,
        time_period,
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
    join {{ref('v_dim_item')}} using (itm_key)
    join {{ref('date_xfrm')}} using (xfrm_date)
    join {{ref('dim_date')}} using (date)
    left join {{ref('lu_price_list_history')}} using (color)
    where
        to_date_type = 'TODAY'
        and time_period in ('TY', 'LY')
        and date between previous_day(current_date(), 'sa') - 6 and current_date() - 1
        and coalesce(effective_date, '1901-01-01') <= xfrm_date
        and coalesce(end_date, '2199-01-01') >= xfrm_date
    group by all
)
select *
from cte_sales