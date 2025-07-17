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
        first_order_date
    from {{ref('dim_customer')}}, lateral flatten (customer_ids) c
),
cte_sales as (
    select
        date,
        order_id,
        channel,
        customer_id,
        sale_qty,
        sale_cost,
        sale_amt
    from {{ref('v_fct_order_items')}}
    where
        date >= '2023-01-29'
),
cte_sales_by_user_type as (
    select
        date,
        channel,
        case
            when first_order_date is null then 'New'
            when first_order_date >= date then 'New'
            else 'Returning'
        end as user_type,
        count(distinct order_id) as orders,
        sum(sale_qty) as sale_qty,
        sum(sale_cost) as sale_cost,
        sum(sale_amt) as sale_amt
    from cte_sales
    left join cte_customer using (customer_id)
    group by all
),
cte_sessions as (
    select
        date,
        channel,
        user_type,
        sum(sessions) as sessions
    from {{ref('fct_sessions_by_channel')}}
    where
        date >= '2023-01-29'
    group by all
),
cte_channel_group as (
    select
        channel,
        channel_group 
    from {{ref('dim_channel')}}
)
select
    date,
    channel,
    channel_group,
    user_type,
    coalesce(sessions, 0) as sessions,
    coalesce(orders, 0) as orders,
    coalesce(sale_qty, 0) as sale_qty,
    coalesce(sale_cost, 0) as sale_cost,
    coalesce(sale_amt, 0) as sale_amt,
from cte_sales_by_user_type
natural full join cte_sessions
left join cte_channel_group using (channel)