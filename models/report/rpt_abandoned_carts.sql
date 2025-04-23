{{
    config(
        materialized = 'view'
    )
}}
with
cte_add_to_cart as (
    select
        date,
        sum(users) as users_with_add_to_cart
    from {{ref('fct_add_to_cart_users')}}
    group by all
),
cte_purchases as (
    select
        date,
        count(distinct order_id) as conversions
    from {{ref('v_fct_order_items')}}
    where
        date >= (select min(date) from {{ref('fct_add_to_cart_users')}})
        and date < current_date()
    group by all
)
select
    date,
    coalesce(users_with_add_to_cart, 0) as users_with_add_to_cart,
    coalesce(conversions, 0) as conversions
from cte_add_to_cart
natural full join cte_purchases