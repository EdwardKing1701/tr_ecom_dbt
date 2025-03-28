{{
    config(
        materialized = 'view'
    )
}}
select
    meas_dt as date,
    itm_key,
    sku,
    attr_col_1 as channel,
    attr_col_2 as order_id,
    attr_col_11 as order_type,
    attr_col_12 as platform,
    f_meas_qty as sale_qty,
    f_meas_cst as sale_cost,
    f_meas_rtl as sale_amt
from {{source('robling_merch', 'dv_dm_f_meas_il_b')}}
natural left join cte_hour
where
    meas_cde = 'CO_ORDERED'
    {# and itm_key <> 565 -- gift cards #}