{{
    config(
        materialized = 'table',
        pk = ['date', 'channel', 'platform']
    )
}}
with
cte_analytics_channel as (
    select
        date,
        channel,
        sum(sessions) as sessions_unadjusted,
        sum(engaged_sessions) as engaged_sessions_unadjusted,
        sum(purchases) as orders_unadjusted,
        sum(coalesce(quantity, 0)) as sale_qty_unadjusted,
        sum(revenue) as sale_amt_unadjusted,
        sum(shipping) as shipping_unadjusted,
        sum(tax) as tax_unadjusted
    from {{ref('src_ga_channels')}}
    full join {{ref('src_ga_items')}} using (date, channel_original, channel)
    where
        channel <> 'Unassigned'
    group by all
),
cte_analytics_session as (
    select
        date,
        coalesce(sessions, 0) as sessions_total,
        coalesce(engaged_sessions, 0) as engaged_sessions_total
    from {{ref('src_ga_sessions')}}
),
cte_demand_sales as (
    select
        date,
        count(distinct order_id) as orders_total,
        sum(sale_qty) as sale_qty_total,
        sum(sale_amt) as sale_amt_total,
        sum(shipping) as shipping_total,
        sum(tax) as tax_total
    from {{ref('v_fct_orders')}}
    group by all
),
cte_adjusted_demand as (
    select
        date,
        channel,
        sessions_total * ratio_to_report(sessions_unadjusted) over (partition by date) as sessions,
        engaged_sessions_total * ratio_to_report(engaged_sessions_unadjusted) over (partition by date) as engaged_sessions,
        orders_total * ratio_to_report(orders_unadjusted) over (partition by date) as orders,
        sale_qty_total * ratio_to_report(sale_qty_unadjusted) over (partition by date) as sale_qty,
        sale_amt_total * ratio_to_report(sale_amt_unadjusted) over (partition by date) as sale_amt,
        shipping_total * ratio_to_report(shipping_unadjusted) over (partition by date) as shipping,
        tax_total * ratio_to_report(tax_unadjusted) over (partition by date) as tax,
        sessions_unadjusted,
        engaged_sessions_unadjusted,
        orders_unadjusted,
        sale_qty_unadjusted,
        sale_amt_unadjusted,
        shipping_unadjusted,
        tax_unadjusted
    from cte_analytics_channel
    natural join cte_demand_sales
    natural join cte_analytics_session
),
cte_platform_setup as (
    select
        'Android' as platform
    union all
    select
        'iOS' as platform
    union all
    select
        'web' as platform
),
cte_platform as (
    select
        date,
        channel,
        platform,
        sum(sessions) as platform_sessions,
        sum(engaged_sessions) as platform_engaged_sessions,
        sum(purchases) as platform_orders,
        sum(coalesce(quantity, 0)) as platform_sale_qty,
        sum(revenue) as platform_sale_amt,
        sum(shipping) as platform_shipping,
        sum(tax) as platform_tax
    from {{ref('src_ga_channels_platform')}}
    full join {{ref('src_ga_items_platform')}} using (date, channel_original, channel, platform)
    where
        platform in ('Android', 'iOS', 'web')
        and channel <> 'Unassigned'
    group by all
),
cte_platform_ratio as (
    select
        date,
        channel,
        platform,
        sessions,
        engaged_sessions,
        orders,
        sale_qty,
        sale_amt,
        shipping,
        tax,
        sessions_unadjusted,
        engaged_sessions_unadjusted,
        orders_unadjusted,
        sale_qty_unadjusted,
        sale_amt_unadjusted,
        shipping_unadjusted,
        tax_unadjusted,
        ratio_to_report(coalesce(platform_sessions, 0)) over (partition by date, channel) as platform_sessions_ratio,
        ratio_to_report(coalesce(platform_engaged_sessions, 0)) over (partition by date, channel) as platform_engaged_sessions_ratio,
        ratio_to_report(coalesce(platform_orders, 0)) over (partition by date, channel) as platform_orders_ratio,
        ratio_to_report(coalesce(platform_sale_qty, 0)) over (partition by date, channel) as platform_sale_qty_ratio,
        ratio_to_report(coalesce(platform_sale_amt, 0)) over (partition by date, channel) as platform_sale_amt_ratio,
        ratio_to_report(coalesce(platform_shipping, 0)) over (partition by date, channel) as platform_shipping_ratio,
        ratio_to_report(coalesce(platform_tax, 0)) over (partition by date, channel) as platform_tax_ratio
    from cte_adjusted_demand
    join cte_platform_setup
    left join cte_platform using (date, channel, platform)
)
select
    date,
    channel,
    platform,
    case
        when sessions = 0 then 0
        when platform_sessions_ratio is null and sessions > 0 and platform = 'web' then sessions
        when platform_sessions_ratio is not null then sessions * platform_sessions_ratio
        else 0
    end as sessions,
    case
        when engaged_sessions = 0 then 0
        when platform_engaged_sessions_ratio is null and engaged_sessions > 0 and platform = 'web' then engaged_sessions
        when platform_engaged_sessions_ratio is not null then engaged_sessions * platform_engaged_sessions_ratio
        else 0
    end as engaged_sessions,
    case
        when orders = 0 then 0
        when platform_orders_ratio is null and orders > 0 and platform = 'web' then orders
        when platform_orders_ratio is not null then orders * platform_orders_ratio
        else 0
    end as orders,
    case
        when sale_qty = 0 then 0
        when platform_sale_qty_ratio is null and sale_qty > 0 and platform = 'web' then sale_qty
        when platform_sale_qty_ratio is not null then sale_qty * platform_sale_qty_ratio
        else 0
    end as sale_qty,
    case
        when sale_amt = 0 then 0
        when platform_sale_amt_ratio is null and sale_amt > 0 and platform = 'web' then sale_amt
        when platform_sale_amt_ratio is not null then sale_amt * platform_sale_amt_ratio
        else 0
    end as sale_amt,
    case
        when shipping = 0 then 0
        when platform_shipping_ratio is null and shipping > 0 and platform = 'web' then shipping
        when platform_shipping_ratio is not null then shipping * platform_shipping_ratio
        else 0
    end as shipping,
    case
        when tax = 0 then 0
        when platform_tax_ratio is null and tax > 0 and platform = 'web' then tax
        when platform_tax_ratio is not null then tax * platform_tax_ratio
        else 0
    end as tax,
    case
        when sessions_unadjusted = 0 then 0
        when platform_sessions_ratio is null and sessions_unadjusted > 0 and platform = 'web' then sessions_unadjusted
        when platform_sessions_ratio is not null then sessions_unadjusted * platform_sessions_ratio
        else 0
    end as sessions_unadjusted,
    case
        when engaged_sessions_unadjusted = 0 then 0
        when platform_engaged_sessions_ratio is null and engaged_sessions_unadjusted > 0 and platform = 'web' then engaged_sessions_unadjusted
        when platform_engaged_sessions_ratio is not null then engaged_sessions_unadjusted * platform_engaged_sessions_ratio
        else 0
    end as engaged_sessions_unadjusted,
    case
        when orders_unadjusted = 0 then 0
        when platform_orders_ratio is null and orders_unadjusted > 0 and platform = 'web' then orders_unadjusted
        when platform_orders_ratio is not null then orders_unadjusted * platform_orders_ratio
        else 0
    end as orders_unadjusted,
    case
        when sale_qty_unadjusted = 0 then 0
        when platform_sale_qty_ratio is null and sale_qty_unadjusted > 0 and platform = 'web' then sale_qty_unadjusted
        when platform_sale_qty_ratio is not null then sale_qty_unadjusted * platform_sale_qty_ratio
        else 0
    end as sale_qty_unadjusted,
    case
        when sale_amt_unadjusted = 0 then 0
        when platform_sale_amt_ratio is null and sale_amt_unadjusted > 0 and platform = 'web' then sale_amt_unadjusted
        when platform_sale_amt_ratio is not null then sale_amt_unadjusted * platform_sale_amt_ratio
        else 0
    end as sale_amt_unadjusted,
    case
        when shipping_unadjusted = 0 then 0
        when platform_shipping_ratio is null and shipping_unadjusted > 0 and platform = 'web' then shipping_unadjusted
        when platform_shipping_ratio is not null then shipping_unadjusted * platform_shipping_ratio
        else 0
    end as shipping_unadjusted,
    case
        when tax_unadjusted = 0 then 0
        when platform_tax_ratio is null and tax_unadjusted > 0 and platform = 'web' then tax_unadjusted
        when platform_tax_ratio is not null then tax_unadjusted * platform_tax_ratio
        else 0
    end as tax_unadjusted,
    current_timestamp() as inserted_ts
from cte_platform_ratio
order by date, channel, platform