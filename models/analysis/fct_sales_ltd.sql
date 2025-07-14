{{
    config(
        materialized = 'table',
        pk = ['itm_id']
    )
}}
with
cte_orders as (
    select
        id as order_id,
        case
            when id like 'FL%' then
                creation_date::date
            else
                convert_timezone('America/Los_Angeles', creation_date)::date
        end as order_date,
        creation_date
from tr_prd_db_fivetran.salesforce_commerce_cloud.orders_history
qualify row_number() over (partition by id order by _fivetran_synced desc) = 1
)
select
    product_id as itm_id,
    min(order_date) as first_sale_date,
    max(order_date) as last_sale_date,
    sum(quantity) as sale_qty_ltd,
    sum(price_after_order_discount) as sale_amt_ltd,
    current_timestamp() as inserted_ts
from tr_prd_db_fivetran.salesforce_commerce_cloud.order_product_item
join cte_orders using (order_id)
group by all
order by first_sale_date, itm_id