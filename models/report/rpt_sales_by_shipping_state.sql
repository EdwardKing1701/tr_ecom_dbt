{{
    config(
        materialized = 'view'
    )
}}
with
cte_shipping_state as (
    select
        order_id,
        country_code,
        state_code
    from {{source('sfcc', 'order_shipment_address')}}
),
cte_lu_state as (
    select
        state_code,
        state_name,
        subdivision_category
    from {{ref('lu_state')}}
),
cte_sales as (
    select
        date,
        order_id,
        order_type,
        sale_amt,
        sale_qty
    from {{ref('v_fct_order_items')}}
)
select
    date,
    case
        when order_type in ('Facebook', 'International') then order_type
        else 'US'
    end as country_code,
    case
        when order_type in ('Facebook', 'International') then order_type
        when subdivision_category is not null then state_code
        else '(N/A)'
    end as state_code,
    coalesce(subdivision_category, '(N/A)') as subdivision_category,
    count(distinct order_id) as orders,
    sum(sale_amt) as sale_amt,
    sum(sale_qty) as sale_qty
from cte_sales
left join cte_shipping_state using (order_id)
left join cte_lu_state using (state_code)
group by all