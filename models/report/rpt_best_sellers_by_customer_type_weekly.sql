{{
    config(
        materialized = 'view'
    )
}}
with
cte_customer as (
    select
        c.value as customer_id,
        email_address,
        first_order_date,
        last_order_date,
        orders_ltd,
        sale_cost_ltd,
        sale_qty_ltd,
        sale_amt_ltd
    from {{ref('dim_customer')}}, lateral flatten (customer_ids) c
),
cte_sales as (
    select
        date as xfrm_date,
        itm_key,
        channel,
        order_id,
        customer_id,
        order_type,
        sale_qty,
        sale_cost,
        sale_amt
    from {{ref('v_fct_order_items')}}
),
cte_catalogue as (
    select
        itm_key,
        sku,
        color,
        style,
        style_desc,
        division,
        department,
        class,
        subclass
    from {{ref('dim_item')}}
),
cte_price_category as (
    select
        color,
        date as xfrm_date,
        price_category
    from {{ref('lu_price_list_history_by_day')}}
),
cte_calendar as (
    select
        date,
        week_id,
        xfrm_date,
        time_period
    from {{ref('date_xfrm')}}
    join {{ref('dim_date')}} using (date)
    where
        to_date_type = 'TODAY'
        and time_period in ('TY', 'LY')
        and date < current_date()
)
select
    time_period,
    week_id,
    case
        when first_order_date is null then '(N/A)'
        when xfrm_date = first_order_date then 'New'
        else 'Returning'
    end as customer_type,
    style,
    style_desc,
    division,
    department,
    class,
    subclass,
    coalesce(price_category, 'REG') as price_category,
    channel,
    order_type,
    null as custom_group,
    sum(sale_qty) as sale_qty,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt
from cte_sales
join cte_calendar using (xfrm_date)
left join cte_catalogue using (itm_key)
left join cte_price_category using (xfrm_date, color)
left join cte_customer using (customer_id)
group by all