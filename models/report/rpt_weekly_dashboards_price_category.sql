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
        order_id,
        customer_id,
        itm_key,
        sale_qty,
        sale_cost,
        sale_amt
    from {{ref('v_fct_order_items')}}
),
cte_basket_gender as (
    select
        order_id,
        sum(iff(division = 'TRBJ WOMENS', sale_qty, 0)) as womens_sale_qty,
        sum(iff(division = 'TRBJ MENS', sale_qty, 0)) as mens_sale_qty
    from cte_sales
    join cte_catalogue using (itm_key)
    group by all
),
cte_customer as (
    select
        c.value as customer_id,
        email_address,
        first_order_date
    from {{ref('dim_customer')}}, lateral flatten (customer_ids) c
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
        and year_id >= (select year_id - 1 from {{ref('dim_date')}} where date = previous_day(current_date(), 'sa'))
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
    case
        when first_order_date is null then '(N/A)'
        when order_date = first_order_date then 'New'
        else 'Returning'
    end as customer_type,
    case
        when womens_sale_qty > 0 and mens_sale_qty > 0 then 'Mixed'
        when womens_sale_qty > 0 then 'Women'
        when mens_sale_qty > 0 then 'Men'
        else '(N/A)'
    end as basket_gender,
    sum(sale_qty) as sale_qty,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt
from cte_sales
join cte_catalogue using (itm_key)
left join cte_price_history using (order_date, color)
left join cte_basket_gender using (order_id)
left join cte_customer using (customer_id)
join cte_calendar using (order_date)
group by all