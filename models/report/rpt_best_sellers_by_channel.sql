{{
    config(
        materialized = 'view'
    )
}}
with
cte_date_range as (
    select
        day_key as meas_dt
    from robling_prd_db.dm_merch_v.dv_dwh_d_tim_day_lu
    where
        day_key between previous_day(current_date(), 'sa') - 6 and previous_day(current_date(), 'sa')
),
cte_catalogue as (
    select
        itm_key,
        itm_id,
        itm_desc,
        sty_id,
        sty_desc,
        color_id,
        color_desc,
        sbc_id,
        sbc_desc,
        cls_id,
        cls_desc,
        dpt_id,
        dpt_desc,
        div_id,
        div_desc,
        size_id,
        size_desc,
        rcd_ins_ts,
        rcd_upd_ts
    from robling_prd_db.dw_dwh.dwh_d_prd_itm_lu
    where
        sty_id <> 'GIFTCARD'
        and itm_key <> 565
),
cte_rep_color_setup as (
    select
        sty_id,
        color_id as rep_color_id,
        color_desc as rep_color_desc,
        sum(f_meas_rtl) as sales
    from robling_prd_db.dm_merch_v.dv_dm_f_meas_il_b
    natural join cte_date_range
    join cte_catalogue using(itm_key)
    where
        meas_cde in ('CO_ORDERED')
    group by all
),
cte_rep_color as (
    select
        sty_id,
        rep_color_id,
        rep_color_desc
    from cte_rep_color_setup
    qualify row_number() over (partition by sty_id order by sales desc) = 1
)
select
    meas_dt,
    attr_col_1 as channel,
    attr_col_11 as order_type,
    sty_id,
    sty_desc,
    rep_color_id,
    rep_color_desc,
    sbc_desc,
    cls_desc,
    dpt_desc,
    div_desc,
    sum(f_meas_rtl) as sales,
    sum(f_meas_qty) as units
from robling_prd_db.dm_merch_v.dv_dm_f_meas_il_b
natural join cte_date_range
join cte_catalogue using(itm_key)
join cte_rep_color using(sty_id)
where
    meas_cde in ('CO_ORDERED')
group by all