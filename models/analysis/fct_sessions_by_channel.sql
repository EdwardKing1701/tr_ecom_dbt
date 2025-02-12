{{
    config(
        materialized = 'table',
        pk = ['date', 'channel']
    )
}}
with
cte_analytics as (
    select
        date,
        channel,
        coalesce(sessions, 0) as sessions,
        coalesce(engaged_sessions, 0) as engaged_sessions,
        coalesce(revenue, 0) as analytics_demand,
        coalesce(purchases, 0) as analytics_orders,
        coalesce(quantity, 0) as analytics_units,
        coalesce(shipping, 0) as analytics_shipping,
        coalesce(tax, 0) as analytics_tax
    from {{ref('ga_sessions')}}
    full join {{ref('ga_items')}} using(date, channel)
),
cte_demand as (
    select
        meas_dt as date,
        sum(iff(upper(meas_cde) = 'CO_ORDERED', f_meas_rtl, 0)) as demand_total,
        count(distinct iff(upper(meas_cde) = 'CO_ORDERED', attr_col_2, null)) as orders_total,
        sum(iff(upper(meas_cde) = 'CO_ORDERED', f_meas_qty, 0)) as units_total,
        sum(iff(upper(meas_cde) = 'DMD', f_meas_col1, 0)) as shipping_total,
        sum(iff(upper(meas_cde) = 'DMD', f_meas_col5, 0)) as tax_total
    from robling_prd_db.dm_merch_v.dv_dm_f_meas_il_b
    where
        meas_cde in (null, 'CO_ORDERED', 'DMD')
    group by all
),
cte_adjusted_demand as (
    select
        date,
        channel,
        sessions,
        engaged_sessions,
        analytics_demand,
        demand_total * ratio_to_report(analytics_demand) over (partition by date) as adjusted_demand,
        analytics_orders,
        orders_total * ratio_to_report(analytics_orders) over (partition by date) as adjusted_orders,
        analytics_units,
        units_total * ratio_to_report(analytics_units) over (partition by date) as adjusted_units,
        analytics_shipping,
        shipping_total * ratio_to_report(analytics_shipping) over (partition by date) as adjusted_shipping,
        analytics_tax,
        tax_total * ratio_to_report(analytics_tax) over (partition by date) as adjusted_tax
    from cte_analytics
    join cte_demand using(date)
)
select
    *,
    current_timestamp() as inserted_ts
from cte_adjusted_demand
order by 1