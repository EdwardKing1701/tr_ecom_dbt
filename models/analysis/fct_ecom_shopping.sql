{{
    config(
        materialized = 'table',
        pk = ['date', 'platform', 'style', 'color', 'item_list_id']
    )
}}
select
    event_date as date,
    'web' as platform,
    i.value:item_id::text as style,
    i.value:item_variant::text as color,
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
    sum(iff(event_name = 'purchase', i.value:item_revenue_in_usd::float, 0)) as purchase_amt,
    current_timestamp() as inserted_ts
from {{source('google_analytics', 'analytics_303711007__view')}}, lateral flatten (items) i
where
    event_name in ('view_item', 'add_to_cart', 'remove_from_cart', 'begin_checkout', 'purchase')
    and platform = 'WEB'
    and event_date >= '2025-02-02'
    and event_date = current_date() - 2
group by all
order by date, platform, style, color, item_list_id