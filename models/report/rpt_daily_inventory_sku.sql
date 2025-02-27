{{
    config(
        materialized = 'view'
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
from TR_PRD_DB_FIVETRAN.SALESFORCE_COMMERCE_CLOUD.ORDERS_HISTORY
where
    order_date >= current_date() - 28
qualify row_number() over (partition by id order by _fivetran_synced desc) = 1
),
cte_order_items as (
    select
        product_id as itm_id,
        sum(iff(order_date between current_date() - 28 and current_date() - 22, price_after_order_discount, 0)) as sale_amt_4wk,
        sum(iff(order_date between current_date() - 21 and current_date() - 15, price_after_order_discount, 0)) as sale_amt_3wk,
        sum(iff(order_date between current_date() - 14 and current_date() - 8, price_after_order_discount, 0)) as sale_amt_2wk,
        sum(iff(order_date between current_date() - 7 and current_date() - 1, price_after_order_discount, 0)) as sale_amt_1wk,
        sum(iff(order_date = current_date(), price_after_order_discount, 0)) as sale_amt_today,
        sum(iff(order_date between current_date() - 28 and current_date() - 22, quantity, 0)) as sale_qty_4wk,
        sum(iff(order_date between current_date() - 21 and current_date() - 15, quantity, 0)) as sale_qty_3wk,
        sum(iff(order_date between current_date() - 14 and current_date() - 8, quantity, 0)) as sale_qty_2wk,
        sum(iff(order_date between current_date() - 7 and current_date() - 1, quantity, 0)) as sale_qty_1wk,
        sum(iff(order_date = current_date(), quantity, 0)) as sale_qty_today
    from TR_PRD_DB_FIVETRAN.SALESFORCE_COMMERCE_CLOUD.ORDER_PRODUCT_ITEM
    join cte_orders using(order_id)
    group by all
),
cte_inventory as (
    select
        product_id as itm_id,
        allocation_amount,
        ats,
        stock_level
    from TR_PRD_DB_FIVETRAN.SALESFORCE_COMMERCE_CLOUD.INVENTORY_LIST_RECORD
    where
        inventory_list_id = 'dfs-inv-list'
        and not _fivetran_deleted
),
cte_ltd as (
    select
        itm_id,
        first_sale_date,
        last_sale_date,
        sale_qty_ltd,
        sale_amt_ltd
    from {{ref('fct_sales_ltd')}}
)
cte_sfcc_master as (
    select
        id as sty_id,
        searchable_default as searchable_flag,
        short_description_default_source as item_description,
        c_promo_price as promo_price,
        primary_category_id,
        page_title_default as page_title,
        name_default as item_name,
        page_description_default as page_description,
        c_total_reviews as total_reviews,
        c_average_score as avg_score,
        c_search_keywords as keywords,
        c_cc_fabric_content as fabric_content,
        c_badge as badge
    from TR_PRD_DB_FIVETRAN.SALESFORCE_COMMERCE_CLOUD.PRODUCT
    where
        owning_catalog_id  = 'tr-master-catalog'
        and type_master
),
cte_sfcc_variant as (
    select
        id as itm_id,
        creation_date,
        last_modified,
        online_flag_default as online_flag,
        price
    from TR_PRD_DB_FIVETRAN.SALESFORCE_COMMERCE_CLOUD.PRODUCT
    where
        owning_catalog_id  = 'tr-master-catalog'
        and type_variant
),
cte_catalogue as (
    select
        itm_id,
        color_id,
        color_desc,
        size_id,
        size_desc,
        sty_id,
        sty_desc,
        div_id,
        div_desc,
        dpt_id,
        dpt_desc,
        cls_id,
        cls_desc,
        sbc_id,
        sbc_desc
    from ROBLING_PRD_DB.DM_MERCH_V.DV_DWH_D_PRD_ITM_LU
)
select
    itm_id,
    sty_id || '-' || color_id as item_color_id,
    sty_id,
    color_id,
    color_desc,
    size_desc,
    sty_desc,
    div_desc,
    dpt_desc,
    cls_desc,
    sbc_desc,
    creation_date,
    last_modified,
    online_flag,
    searchable_flag,
    price,
    promo_price,
    item_name,
    item_description,
    keywords,
    primary_category_id,
    page_title,
    page_description,
    total_reviews,
    avg_score,
    {# fabric_content, #}
    badge,
    {# allocation_amount, #}
    ats as current_inv,
    {# stock_level, #}
    coalesce(sale_qty_today, 0) as sale_qty_today,
    coalesce(sale_qty_1wk, 0) as sale_qty_1wk,
    coalesce(sale_qty_2wk, 0) as sale_qty_2wk,
    coalesce(sale_qty_3wk, 0) as sale_qty_3wk,
    coalesce(sale_qty_4wk, 0) as sale_qty_4wk,
    coalesce(sale_qty_ltd, 0) as sale_qty_ltd,
    sale_qty_1wk / nullifzero(sale_qty_1wk + ats) as st_1wk,
    (sale_qty_1wk + sale_qty_2wk + sale_qty_3wk + sale_qty_4wk) / nullifzero(sale_qty_1wk + sale_qty_2wk + sale_qty_3wk + sale_qty_4wk + ats) as st_4wks,
    sale_qty_ltd / nullifzero(sale_qty_ltd + ats) as st_ltd,
    {# coalesce(sale_amt_today, 0) as sale_amt_today,
    coalesce(sale_amt_1wk, 0) as sale_amt_1wk,
    coalesce(sale_amt_2wk, 0) as sale_amt_2wk,
    coalesce(sale_amt_3wk, 0) as sale_amt_3wk,
    coalesce(sale_amt_4wk, 0) as sale_amt_4wk, #}
from cte_order_items
full join cte_inventory using(itm_id)
left join cte_catalogue using(itm_id)
left join cte_sfcc_variant using(itm_id)
left join cte_sfcc_master using(sty_id)
left join cte_ltd using(itm_id)