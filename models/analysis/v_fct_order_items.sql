{{
    config(
        materialized = 'view'
    )
}}
with
cte_sales as (
    select
        meas_dt as date,
        itm_key,
        min_key,
        attr_varchar_col1 as channel,
        co_id as order_id,
        trim(lower(attr_varchar_col7)) as customer_id,
        attr_varchar_col11 as order_type,
        attr_varchar_col12 as platform,
        f_meas_qty as sale_qty,
        f_meas_cst as sale_cost,
        f_meas_rtl as sale_amt
    from {{source('robling_merch', 'dv_dm_f_meas_il_b')}}
    where
        meas_cde = 'CO_ORDERED'
),
cte_hour as (
    select
        min_key,
        hr_24hr_id as hour
    from {{source('robling_merch', 'dv_dwh_d_tim_min_of_day_lu')}}
),
cte_channel_correction as (
    select
        channel,
        channel_correction
    from {{ref('channel_correction')}}
)
select
    date,
    to_timestamp_tz(date::varchar || ' ' || left(hour, 2) || ':00:00.000') as order_ts,
    order_id,
    customer_id,
    order_type,
    coalesce(channel_correction, channel) as channel,
    platform,
    itm_key,
    sale_qty,
    sale_cost,
    sale_amt,
from cte_sales
natural left join cte_hour
natural left join cte_channel_correction