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
    co_id as order_id,
    min(meas_dt) as date,
    min(to_timestamp_tz(meas_dt::varchar || ' ' || left(hour, 2) || ':00:00.000')) as order_ts,
    min(min_key) as min_key,
    min(cus_profile_key) as customer_key,
    min(trim(lower(attr_varchar_col7))) as customer_id,
    min(attr_varchar_col7) as channel,
    min(attr_varchar_col11) as order_type,
    min(attr_varchar_col12) as platform,
    sum(iff(meas_cde = 'CO_ORDERED', f_meas_qty, 0)) as sale_qty,
    sum(iff(meas_cde = 'CO_ORDERED', f_meas_cst, 0)) as sale_cost,
    sum(iff(meas_cde = 'CO_ORDERED', f_meas_rtl, 0)) as sale_amt,
    sum(iff(upper(meas_cde) = 'DMD', f_fact_amt1, 0)) as shipping,
    sum(iff(upper(meas_cde) = 'DMD', f_fact_amt5, 0)) as tax,
    sum(iff(upper(meas_cde) = 'DMD', f_fact_amt4, 0)) as route_protection_fee
from {{source('robling_merch', 'dv_dm_f_meas_il_b')}}
natural left join cte_hour
where
    meas_cde in ('CO_ORDERED', 'DMD')
    and co_id is not null
    and meas_dt is not null
group by 1