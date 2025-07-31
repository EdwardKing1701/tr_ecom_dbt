{{
    config(
        materialized = 'view'
    )
}}
with
cte_sales_total as (
    select
        demand_date as date,
        'Total' as country_code,
        count(distinct order_id) as orders,
        sum(sale_qty) as sale_qty,
        sum(sale_amt) as sale_amt,
        sum(sale_amt) - sum(sale_cost) as gross_margin
    from {{ref('stg_sfcc_orders')}}
    where
        order_type = 'International'
        and status <> 'cancelled'
    group by all
),
cte_traffic_total as (
    select
        date,
        'Total' as country_code,
        sessions
    from {{ref('ga_international')}}
),
cte_sales_by_country as (
    select
        demand_date as date,
        shipping_country as country_code,
        count(distinct order_id) as orders,
        sum(sale_qty) as sale_qty,
        sum(sale_amt) as sale_amt,
        sum(sale_amt) - sum(sale_cost) as gross_margin
    from {{ref('stg_sfcc_orders')}}
    where
        order_type = 'International'
    group by all
),
cte_traffic_by_country as (
    select
        date,
        country_code,
        sessions
    from {{ref('ga_international_by_country')}}

),
cte_country as (
    select
        country_code,
        country_name
    from {{ref('lu_country')}}
)
select
    date,
    country_code,
    country_name,
    coalesce(orders, 0) as orders,
    coalesce(sale_qty, 0) as sale_qty,
    coalesce(sale_amt, 0) as sale_amt,
    coalesce(gross_margin, 0) as gross_margin,
    coalesce(sessions, 0) as sessions
from cte_sales_total
full join cte_traffic_total using (date, country_code)
left join cte_country using (country_code)
where
    date < current_date()

union all

select
    date,
    country_code,
    country_name,
    coalesce(orders, 0) as orders,
    coalesce(sale_qty, 0) as sale_qty,
    coalesce(sale_amt, 0) as sale_amt,
    coalesce(gross_margin, 0) as gross_margin,
    coalesce(sessions, 0) as sessions
from cte_sales_by_country
full join cte_traffic_by_country using (date, country_code)
left join cte_country using (country_code)
where
    date < current_date()