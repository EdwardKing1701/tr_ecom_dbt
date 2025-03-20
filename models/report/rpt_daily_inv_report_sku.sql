{{
    config(
        materialized = 'view'
    )
}}
with
cte_sales_and_inv as (
    select
        sku,
        inventory_class,
        season_code,
        first_receipt_date,
        last_receipt_date,
        first_sale_date,
        inventory_units,
        inventory_cost,
        inventory_retail,
        net_sales_units_ltd,
        net_sales_cost_ltd,
        net_sales_retail_ltd,
        net_sales_units_4wk,
        net_sales_units_3wk,
        net_sales_units_2wk,
        net_sales_units_1wk,
        net_sales_units_yest,
        net_sales_cost_4wk,
        net_sales_cost_3wk,
        net_sales_cost_2wk,
        net_sales_cost_1wk,
        net_sales_cost_yest,
        net_sales_retail_4wk,
        net_sales_retail_3wk,
        net_sales_retail_2wk,
        net_sales_retail_1wk,
        net_sales_retail_yest,
        dmd_sales_units_4wk,
        dmd_sales_units_3wk,
        dmd_sales_units_2wk,
        dmd_sales_units_1wk,
        dmd_sales_units_yest,
        dmd_sales_cost_4wk,
        dmd_sales_cost_3wk,
        dmd_sales_cost_2wk,
        dmd_sales_cost_1wk,
        dmd_sales_cost_yest,
        dmd_sales_retail_4wk,
        dmd_sales_retail_3wk,
        dmd_sales_retail_2wk,
        dmd_sales_retail_1wk,
        dmd_sales_retail_yest
    from tr_prd_ecom_db.report.rpt_daily_inv_report_setup
),
cte_inv_sfcc as (
    select
        product_id::varchar as sku,
        allocation_amount,
        ats,
        stock_level
    from tr_prd_db_fivetran.salesforce_commerce_cloud.inventory_list_record
    where
        inventory_list_id = 'dfs-inv-list'
        and not _fivetran_deleted
),
cte_sfcc_master as (
    select
        id::varchar as style,
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
    from tr_prd_db_fivetran.salesforce_commerce_cloud.product
    where
        owning_catalog_id  = 'tr-master-catalog'
        and type_master
),
cte_sfcc_variant as (
    select
        id::varchar as sku,
        convert_timezone('America/Los_Angeles', creation_date)::date as created_date,
        convert_timezone('America/Los_Angeles', last_modified)::date as modified_date,
        online_flag_default as online_flag
    from tr_prd_db_fivetran.salesforce_commerce_cloud.product
    where
        owning_catalog_id  = 'tr-master-catalog'
        and type_variant is not null
        and regexp_like(id, '[0-9]+')
),
cte_catalogue as (
    select
        sku,
        color,
        style,
        size,
        style_desc,
        division,
        department,
        class,
        subclass
    from tr_prd_ecom_db.analysis.v_dim_item
),
cte_demand_sales_today as (
    select
        sku,
        sum(iff(date = current_date(), sale_qty, 0)) as dmd_sales_units_today,
        sum(iff(date = current_date(), sale_cost, 0)) as dmd_sales_cost_today,
        sum(iff(date = current_date(), sale_amt, 0)) as dmd_sales_retail_today
    from tr_prd_ecom_db.analysis.v_fct_order_items
    join tr_prd_ecom_db.analysis.v_dim_item using(itm_key)
    where
        date = current_date()
    group by all
),
cte_new_arrival as (
    select distinct
        style,
        is_new_arrival
    from tr_prd_ecom_db.analysis.lu_assigned_category
    join tr_prd_ecom_db.analysis.dim_category using(category_id)
    where
        is_new_arrival
),
cte_price_file as (
    select
        style,
        color,
        price_list,
        sale_price,
        price_category,
        notes,
        holds
    from tr_prd_ecom_db.analysis.lu_price_list_history
    where
        price_to_date = '2199-01-01'
)
select
    sku,
    color,
    style,
    size,
    style_desc,
    division,
    department,
    class,
    subclass,

    searchable_flag,
    item_description,
    promo_price,
    primary_category_id,
    page_title,
    item_name,
    page_description,
    total_reviews,
    avg_score,
    keywords,
    fabric_content,
    badge,

    created_date,
    modified_date,
    online_flag,

    price_list,
    sale_price,
    price_category,
    notes,
    holds,
    is_new_arrival,

    allocation_amount,
    ats,
    stock_level,

    inventory_class,
    season_code,
    first_receipt_date,
    last_receipt_date,
    first_sale_date,
    inventory_units,
    inventory_cost,
    inventory_retail,
    net_sales_units_ltd,
    net_sales_cost_ltd,
    net_sales_retail_ltd,
    net_sales_units_4wk,
    net_sales_units_3wk,
    net_sales_units_2wk,
    net_sales_units_1wk,
    net_sales_units_yest,
    net_sales_cost_4wk,
    net_sales_cost_3wk,
    net_sales_cost_2wk,
    net_sales_cost_1wk,
    net_sales_cost_yest,
    net_sales_retail_4wk,
    net_sales_retail_3wk,
    net_sales_retail_2wk,
    net_sales_retail_1wk,
    net_sales_retail_yest,
    dmd_sales_units_4wk,
    dmd_sales_units_3wk,
    dmd_sales_units_2wk,
    dmd_sales_units_1wk,
    dmd_sales_units_yest,
    dmd_sales_cost_4wk,
    dmd_sales_cost_3wk,
    dmd_sales_cost_2wk,
    dmd_sales_cost_1wk,
    dmd_sales_cost_yest,
    dmd_sales_retail_4wk,
    dmd_sales_retail_3wk,
    dmd_sales_retail_2wk,
    dmd_sales_retail_1wk,
    dmd_sales_retail_yest,

    dmd_sales_units_today,
    dmd_sales_cost_today,
    dmd_sales_retail_today

from cte_sales_and_inv
full join cte_demand_sales_today using(sku)
full join cte_inv_sfcc using(sku)
left join cte_catalogue using(sku)
left join cte_sfcc_master using(style)
left join cte_sfcc_variant using(sku)
left join cte_new_arrival using(style)
left join cte_price_file using(style, color)