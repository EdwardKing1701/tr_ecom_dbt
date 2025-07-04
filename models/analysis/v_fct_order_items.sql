{{
    config(
        materialized = 'view'
    )
}}
with
cte_hour as (
    select
        min_key,
        hr_24hr_id as hour
    from {{source('robling_merch', 'dv_dwh_d_tim_min_of_day_lu')}}
)
select
    meas_dt as date,
    itm_key,
    to_timestamp_tz(meas_dt::varchar || ' ' || left(hour, 2) || ':00:00.000') as order_ts,
    attr_varchar_col1 as channel,
    co_id as order_id,
    trim(lower(attr_varchar_col7)) as customer_id,
    attr_varchar_col11 as order_type,
    attr_varchar_col12 as platform,
    f_meas_qty as sale_qty,
    f_meas_cst as sale_cost,
    f_meas_rtl as sale_amt
from {{source('robling_merch', 'dv_dm_f_meas_il_b')}}
natural left join cte_hour
where
    meas_cde = 'CO_ORDERED'