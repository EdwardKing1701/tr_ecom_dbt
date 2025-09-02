{{
    config(
        materialized = 'incremental',
        unique_key = 'order_item_id',
        pk = ['order_item_id']
    )
}}
with
cte_orders as (
    select
        id as order_id,
        site_id,
        status,
        creation_date,
        convert_timezone('America/Los_Angeles', creation_date) as order_ts,
        order_ts::date as order_date,
        c_poquser_agent,
        customer_id,
        c_ge_customer_currency_code,
        try_parse_json(c_flow_total_price_json):currency::text as flow_currency,
        currency,
        export_status,
        invoice_no,
        order_total,
        product_sub_total,
        product_total,
        shipping_total,
        taxation,
        tax_total,
        merchandize_total_tax,
        adjusted_merchandize_total_tax,
        shipping_total_tax,
        adjusted_shipping_total_tax,
        convert_timezone('America/Los_Angeles', _fivetran_synced) as order_synced
    from {{source('sfcc', 'orders_history')}}
    {% if is_incremental() %}
    where _fivetran_synced >= (select dateadd('minute', -90, max(order_item_synced)) from {{ this }})
    {% endif %}
    qualify row_number() over (partition by id order by last_modified desc, _fivetran_synced desc) = 1
),
cte_order_items as (
    select
        order_id,
        item_id as order_item_id,
        shipment_id,
        product_id as sku,
        base_price,
        quantity,
        gross_price,
        net_price,
        price_after_item_discount,
        price_after_order_discount,
        tax,
        adjusted_tax,
        bonus_product_line_item,
        gift,
        convert_timezone('America/Los_Angeles', _fivetran_synced) as order_item_synced
    from {{source('sfcc', 'order_product_item')}}
    {% if is_incremental() %}
    where _fivetran_synced >= (select max(order_item_synced) from {{ this }})
    {% endif %}
),
cte_shipments as (
    select
        order_id,
        shipment_id,
        shipment_no,
        shipping_method_id,
        convert_timezone('America/Los_Angeles', _fivetran_synced) as shipment_synced
    from {{source('sfcc', 'order_shipment')}}
    {% if is_incremental() %}
    where _fivetran_synced >= (select dateadd('minute', -90, max(order_item_synced)) from {{ this }})
    {% endif %}
),
cte_shipment_addresses as (
    select
        order_id,
        shipment_id as shipment_no,
        city as shipping_city,
        country_code as shipping_country,
        state_code as shipping_state,
        convert_timezone('America/Los_Angeles', _fivetran_synced) as shipment_address_synced
    from {{source('sfcc', 'order_shipment_address')}}
    {% if is_incremental() %}
    where _fivetran_synced >= (select dateadd('minute', -90, max(order_item_synced)) from {{ this }})
    {% endif %}
),
cte_cost_historical as (
    select
        itm_key,
        color,
        date as order_date,
        cost_historical
    from {{ref('stg_robling_item_cost_history')}}
),
cte_cost_current as (
    select
        itm_key,
        cost_current
    from {{ref('stg_robling_item_cost_current')}}
),
cte_items as (
    select
        sku,
        itm_key,
        color
    from {{ref('stg_items')}}
)
select
    order_item_id,
    order_id,
    site_id,
    shipment_id,
    shipment_no,
    invoice_no,
    status,
    export_status,
    order_ts,
    order_date,
    case
        when order_id like 'FL%' then
            to_timestamp_tz(creation_date::timestamp_ntz)
        else
            convert_timezone('America/Los_Angeles', creation_date)
    end as demand_ts,
    demand_ts::date as demand_date,
    case
        when c_poquser_agent in ('Android', 'iPhone') then 'App'
        when order_id ilike 'fl%' then 'International'
        else 'Domestic'
    end as order_type,
    case
        when c_poquser_agent = 'Android' then 'Android'
        when c_poquser_agent = 'iPhone' then 'iOS'
        else 'web'
    end as platform,
    customer_id,
    currency as order_currency,
    coalesce(c_ge_customer_currency_code, flow_currency, order_currency) as customer_currency,
    iff(order_id ilike 'fl%', 'International', shipping_method_id) as shipping_method,
    shipping_city,
    shipping_country,
    shipping_state,
    order_total,
    shipping_total,
    tax_total,
    shipping_total_tax,
    itm_key,
    sku,
    base_price,
    quantity as sale_qty,
    {% if is_incremental() %}
        quantity * coalesce(cost_current, 0) as sale_cost,
    {% else %}
        quantity * coalesce(cost_historical, cost_current, 0) as sale_cost,
    {% endif %}
    price_after_order_discount as sale_amt,
    price_after_item_discount - price_after_order_discount as order_discount,
    net_price - price_after_item_discount as item_discount,
    order_discount + item_discount as total_discount,
    adjusted_tax as item_tax,
    bonus_product_line_item,
    gift,
    order_synced,
    order_item_synced,
    shipment_synced,
    shipment_address_synced
from cte_order_items
natural join cte_orders
natural left join cte_shipments
natural left join cte_shipment_addresses
natural left join cte_items
{% if not is_incremental() %}
natural left join cte_cost_historical
{% endif %}
natural left join cte_cost_current
order by order_ts, order_id, shipment_id, order_item_id