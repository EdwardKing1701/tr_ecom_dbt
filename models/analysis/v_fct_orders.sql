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
    attr_col_2 as order_id,
    min(meas_dt) as date,
    min(to_timestamp_tz(meas_dt::varchar || ' ' || left(hour, 2) || ':00:00.000')) as order_ts,
    min(min_key) as min_key,
    min(cus_key) as customer_key,
    min(trim(lower(attr_col_7))) as customer_id,
    min(attr_col_1) as channel,
    min(attr_col_11) as order_type,
    min(attr_col_12) as platform,
    sum(iff(meas_cde = 'CO_ORDERED', f_meas_qty, 0)) as sale_qty,
    sum(iff(meas_cde = 'CO_ORDERED', f_meas_cst, 0)) as sale_cost,
    sum(iff(meas_cde = 'CO_ORDERED', f_meas_rtl, 0)) as sale_amt,
    sum(iff(upper(meas_cde) = 'DMD', f_meas_col1, 0)) as shipping,
    sum(iff(upper(meas_cde) = 'DMD', f_meas_col5, 0)) as tax,
    sum(iff(upper(meas_cde) = 'DMD', f_meas_col4, 0)) as route_protection_fee
from {{source('robling_merch', 'dv_dm_f_meas_il_b')}}
natural left join cte_hour
where
    meas_cde in ('CO_ORDERED', 'DMD')
    and attr_col_2 is not null
    and meas_dt is not null
    {# and itm_key <> 565 -- gift cards #}
group by 1