{{
    config(
        materialized = 'view'
    )
}}
with
cte_calendar as (
    select
        date,
        xfrm_date as demand_date,
        time_period
    from {{ref('date_xfrm')}}
    where
        to_date_type = 'TODAY'
        and time_period in ('TY', 'LY')
        and date between previous_day(current_date(), 'sa') - 6 and previous_day(current_date(), 'sa')
),
cte_order_items as (
    select
        order_item_id,
        order_id,
        date,
        time_period,
        order_type,
        sale_qty * base_price as base_price,
        sale_qty,
        sale_cost,
        sale_amt,
        order_discount,
        item_discount
    from {{ref('stg_sfcc_orders')}}
    join cte_calendar using(demand_date)
),
cte_orders as (
    select
        order_id,
        date,
        time_period,
        order_type,
        sum(base_price) as base_price,
        sum(sale_qty) as sale_qty,
        sum(sale_cost) as sale_cost,
        sum(sale_amt) as sale_amt,
        sum(order_discount) as order_discount,
        sum(item_discount) as item_discount,
    from cte_order_items
    group by all
),
cte_robling_setup as (
    select
        date as demand_date,
        order_id,
        channel
    from {{ref('v_fct_order_items')}}
),
cte_robling as (
    select distinct
        order_id,
        channel
    from cte_robling_setup
    join cte_calendar using(demand_date)
),
cte_item_promos as (
    select
        item_id as order_item_id,
        order_id,
        price_adjustment_type,
        promotion_id,
        campaign_id,
        abs(net_price) as promo_discount
    from {{source('sfcc', 'price_adjustment')}}
    where
        price_adjustment_type = 'PRODUCT_ITEM'
),
cte_order_promos as (
    select
        order_id,
        price_adjustment_type,
        promotion_id,
        campaign_id,
        abs(net_price) as promo_discount
    from {{source('sfcc', 'price_adjustment')}}
    where
        price_adjustment_type = 'ORDER'
),
cte_all_promos as (
    select
        order_id,
        'ALL' as price_adjustment_type,
        listagg(distinct promotion_id, ',') within group (order by promotion_id) as promotion_id,
        listagg(distinct campaign_id, ',') within group (order by campaign_id) as campaign_id,
        sum(abs(net_price)) as promo_discount
    from {{source('sfcc', 'price_adjustment')}}
    where
        price_adjustment_type in ('PRODUCT_ITEM', 'ORDER')
    group by all
)
-- All promotions
select
    date,
    time_period,
    order_type,
    coalesce(channel, 'Unassigned') as channel,
    coalesce(promotion_id, 'No promotion') as promotion_id,
    coalesce(campaign_id, 'No promotion') as campaign_id,
    'ALL' as price_adjustment_type,
    count(distinct order_id) as orders,
    sum(base_price) as base_price,
    sum(sale_qty) as sale_qty,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt,
    sum(promo_discount) as promo_discount
from cte_orders
left join cte_all_promos using (order_id)
left join cte_robling using (order_id)
group by all

union all

-- Order promotions
select
    date,
    time_period,
    order_type,
    coalesce(channel, 'Unassigned') as channel,
    promotion_id,
    campaign_id,
    price_adjustment_type,
    count(distinct order_id) as orders,
    sum(base_price) as base_price,
    sum(sale_qty) as sale_qty,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt,
    sum(promo_discount) as promo_discount
from cte_orders
join cte_order_promos using (order_id)
left join cte_robling using (order_id)
group by all

union all

-- Item promotions
select
    date,
    time_period,
    order_type,
    coalesce(channel, 'Unassigned') as channel,
    promotion_id,
    campaign_id,
    price_adjustment_type,
    count(distinct order_id) as orders,
    sum(base_price) as base_price,
    sum(sale_qty) as sale_qty,
    sum(sale_cost) as sale_cost,
    sum(sale_amt) as sale_amt,
    sum(promo_discount) as promo_discount
from cte_order_items
join cte_item_promos using (order_id, order_item_id)
left join cte_robling using (order_id)
group by all