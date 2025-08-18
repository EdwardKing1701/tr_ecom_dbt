{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'replace',
        unique_key = ['date'],
        pk = ['date', 'platform', 'item_variant_id', 'item_id', 'item_list_id']
    )
}}
with
cte_colors as (
    select distinct
        color as item_variant_id,
        color
    from {{ref('stg_items')}}
    where
        color_sku_index = 1
),
cte_styles as (
    select distinct
        style as item_id,
        style
    from {{ref('stg_items')}}
    where
        style_sku_index = 1
),
cte_ga as (
    select
        event_date as date,
        'web' as platform,
        i.value:item_variant::text as item_variant_id,
        i.value:item_id::text as item_id,
        i.value:item_list_id::text as item_list_id,
        sum(iff(event_name = 'view_item', 1, 0)) as item_views,
        sum(iff(event_name = 'add_to_cart', 1, 0)) as adds_to_cart,
        sum(iff(event_name = 'add_to_cart', i.value:quantity::float, 0)) as quantity_added_to_cart,
        sum(iff(event_name = 'remove_from_cart', 1, 0)) as removes_to_cart,
        sum(iff(event_name = 'remove_from_cart', i.value:quantity::float, 0)) as quantity_removed_to_cart,
        sum(iff(event_name = 'begin_checkout', 1, 0)) as checkouts,
        sum(iff(event_name = 'begin_checkout', i.value:quantity::float, 0)) as quantity_checked_out,
        sum(iff(event_name = 'purchase', 1, 0)) as purchases,
        sum(iff(event_name = 'purchase', i.value:quantity::float, 0)) as quantity_purchased,
        sum(iff(event_name = 'purchase', i.value:item_revenue_in_usd::float, 0)) as purchase_amt
    from {{source('google_analytics', 'analytics_303711007__view')}}, lateral flatten (items) i
    where
        event_name in ('view_item', 'add_to_cart', 'remove_from_cart', 'begin_checkout', 'purchase')
        and platform = 'WEB'
        and event_date >= '2025-02-02'
        and event_date < current_date()
        {% if is_incremental() %}
        and event_date >= current_date() - 7
        {% endif %}
    group by all
)
select
    date,
    platform,
    item_variant_id,
    item_id,
    item_list_id,
    coalesce(color, '(N/A)') as color,
    coalesce(style, '(N/A)') as style,
    item_views,
    adds_to_cart,
    quantity_added_to_cart,
    removes_to_cart,
    quantity_removed_to_cart,
    checkouts,
    quantity_checked_out,
    purchases,
    quantity_purchased,
    purchase_amt,
    current_timestamp() as inserted_ts
from cte_ga
left join cte_colors using (item_variant_id)
left join cte_styles using (item_id)
order by date, platform, item_variant_id, item_id, item_list_id