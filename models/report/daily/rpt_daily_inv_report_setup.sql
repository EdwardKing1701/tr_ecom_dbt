{{
    config(
        materialized = 'table',
        pk = ['sku']
    )
}}
with
cte_items as (
    select
        sku::varchar as sku,
        inventory_class,
        season_code,
        original_price,
        min(first_receipt_date) as first_receipt_date,
        max(last_receipt_date) as last_receipt_date,
        min(first_sale_date) as first_sale_date,
        sum(iff(channel = '004:ECOM FULL PRICE', net_sales_units_ltd, 0)) as net_sales_units_ltd,
        sum(iff(channel = '004:ECOM FULL PRICE', net_sales_units_ltd, 0)) as net_sales_cost_ltd,
        sum(iff(channel = '004:ECOM FULL PRICE', net_sales_units_ltd, 0)) as net_sales_retail_ltd
    from {{ref('mstr_daily_items')}}
    where
        sku is not null
    group by all
),
cte_net_sales as (
    select
        sku::varchar as sku,
        sum(iff(date between current_date() - 28 and current_date() - 22, net_sales_units, 0)) as net_sales_units_4wk,
        sum(iff(date between current_date() - 21 and current_date() - 15, net_sales_units, 0)) as net_sales_units_3wk,
        sum(iff(date between current_date() - 14 and current_date() - 8, net_sales_units, 0)) as net_sales_units_2wk,
        sum(iff(date between current_date() - 7 and current_date() - 1, net_sales_units, 0)) as net_sales_units_1wk,
        sum(iff(date = current_date() - 1, net_sales_units, 0)) as net_sales_units_yest,
        sum(iff(date between current_date() - 28 and current_date() - 22, net_sales_cost, 0)) as net_sales_cost_4wk,
        sum(iff(date between current_date() - 21 and current_date() - 15, net_sales_cost, 0)) as net_sales_cost_3wk,
        sum(iff(date between current_date() - 14 and current_date() - 8, net_sales_cost, 0)) as net_sales_cost_2wk,
        sum(iff(date between current_date() - 7 and current_date() - 1, net_sales_cost, 0)) as net_sales_cost_1wk,
        sum(iff(date = current_date() - 1, net_sales_cost, 0)) as net_sales_cost_yest,
        sum(iff(date between current_date() - 28 and current_date() - 22, net_sales_retail, 0)) as net_sales_retail_4wk,
        sum(iff(date between current_date() - 21 and current_date() - 15, net_sales_retail, 0)) as net_sales_retail_3wk,
        sum(iff(date between current_date() - 14 and current_date() - 8, net_sales_retail, 0)) as net_sales_retail_2wk,
        sum(iff(date between current_date() - 7 and current_date() - 1, net_sales_retail, 0)) as net_sales_retail_1wk,
        sum(iff(date = current_date() - 1, net_sales_retail, 0)) as net_sales_retail_yest
    from {{ref('mstr_daily_sales')}}
    where
        channel = '004:ECOM FULL PRICE'
    group by all
),
cte_demand_sales as (
    select
        sku::varchar as sku,
        sum(iff(date between current_date() - 28 and current_date() - 22, sale_qty, 0)) as dmd_sales_units_4wk,
        sum(iff(date between current_date() - 21 and current_date() - 15, sale_qty, 0)) as dmd_sales_units_3wk,
        sum(iff(date between current_date() - 14 and current_date() - 8, sale_qty, 0)) as dmd_sales_units_2wk,
        sum(iff(date between current_date() - 7 and current_date() - 1, sale_qty, 0)) as dmd_sales_units_1wk,
        sum(iff(date = current_date() - 1, sale_qty, 0)) as dmd_sales_units_yest,
        sum(iff(date between current_date() - 28 and current_date() - 22, sale_cost, 0)) as dmd_sales_cost_4wk,
        sum(iff(date between current_date() - 21 and current_date() - 15, sale_cost, 0)) as dmd_sales_cost_3wk,
        sum(iff(date between current_date() - 14 and current_date() - 8, sale_cost, 0)) as dmd_sales_cost_2wk,
        sum(iff(date between current_date() - 7 and current_date() - 1, sale_cost, 0)) as dmd_sales_cost_1wk,
        sum(iff(date = current_date() - 1, sale_cost, 0)) as dmd_sales_cost_yest,
        sum(iff(date between current_date() - 28 and current_date() - 22, sale_amt, 0)) as dmd_sales_retail_4wk,
        sum(iff(date between current_date() - 21 and current_date() - 15, sale_amt, 0)) as dmd_sales_retail_3wk,
        sum(iff(date between current_date() - 14 and current_date() - 8, sale_amt, 0)) as dmd_sales_retail_2wk,
        sum(iff(date between current_date() - 7 and current_date() - 1, sale_amt, 0)) as dmd_sales_retail_1wk,
        sum(iff(date = current_date() - 1, sale_amt, 0)) as dmd_sales_retail_yest
    from {{ref('v_fct_order_items')}}
    join {{ref('v_dim_item')}} using(itm_key)
    where
        date between current_date() - 28 and current_date() - 1
        and channel = '004:ECOM FULL PRICE'
    group by all

),
cte_inventory as (
    select
        sku::varchar as sku,
        sum(inventory_units) as inventory_units,
        sum(inventory_cost) as inventory_cost,
        sum(inventory_retail) as inventory_retail
    from {{ref('mstr_daily_inventory')}}
    where
        channel = '004:ECOM FULL PRICE'
        and date = current_date() - 1
    group by all
)
select
    sku::varchar as sku,
    inventory_class,
    season_code,
    first_receipt_date,
    last_receipt_date,
    first_sale_date,
    inventory_units,
    inventory_cost,
    inventory_retail,
    coalesce(net_sales_units_ltd, 0) as net_sales_units_ltd,
    coalesce(net_sales_cost_ltd, 0) as net_sales_cost_ltd,
    coalesce(net_sales_retail_ltd, 0) as net_sales_retail_ltd,
    coalesce(net_sales_units_4wk, 0) as net_sales_units_4wk,
    coalesce(net_sales_units_3wk, 0) as net_sales_units_3wk,
    coalesce(net_sales_units_2wk, 0) as net_sales_units_2wk,
    coalesce(net_sales_units_1wk, 0) as net_sales_units_1wk,
    coalesce(net_sales_units_yest, 0) as net_sales_units_yest,
    coalesce(net_sales_cost_4wk, 0) as net_sales_cost_4wk,
    coalesce(net_sales_cost_3wk, 0) as net_sales_cost_3wk,
    coalesce(net_sales_cost_2wk, 0) as net_sales_cost_2wk,
    coalesce(net_sales_cost_1wk, 0) as net_sales_cost_1wk,
    coalesce(net_sales_cost_yest, 0) as net_sales_cost_yest,
    coalesce(net_sales_retail_4wk, 0) as net_sales_retail_4wk,
    coalesce(net_sales_retail_3wk, 0) as net_sales_retail_3wk,
    coalesce(net_sales_retail_2wk, 0) as net_sales_retail_2wk,
    coalesce(net_sales_retail_1wk, 0) as net_sales_retail_1wk,
    coalesce(net_sales_retail_yest, 0) as net_sales_retail_yest,
    coalesce(dmd_sales_units_4wk, 0) as dmd_sales_units_4wk,
    coalesce(dmd_sales_units_3wk, 0) as dmd_sales_units_3wk,
    coalesce(dmd_sales_units_2wk, 0) as dmd_sales_units_2wk,
    coalesce(dmd_sales_units_1wk, 0) as dmd_sales_units_1wk,
    coalesce(dmd_sales_units_yest, 0) as dmd_sales_units_yest,
    coalesce(dmd_sales_cost_4wk, 0) as dmd_sales_cost_4wk,
    coalesce(dmd_sales_cost_3wk, 0) as dmd_sales_cost_3wk,
    coalesce(dmd_sales_cost_2wk, 0) as dmd_sales_cost_2wk,
    coalesce(dmd_sales_cost_1wk, 0) as dmd_sales_cost_1wk,
    coalesce(dmd_sales_cost_yest, 0) as dmd_sales_cost_yest,
    coalesce(dmd_sales_retail_4wk, 0) as dmd_sales_retail_4wk,
    coalesce(dmd_sales_retail_3wk, 0) as dmd_sales_retail_3wk,
    coalesce(dmd_sales_retail_2wk, 0) as dmd_sales_retail_2wk,
    coalesce(dmd_sales_retail_1wk, 0) as dmd_sales_retail_1wk,
    coalesce(dmd_sales_retail_yest, 0) as dmd_sales_retail_yest,
    current_timestamp() as inserted_ts
from cte_items
full join cte_net_sales using(sku)
full join cte_demand_sales using(sku)
full join cte_inventory using(sku)