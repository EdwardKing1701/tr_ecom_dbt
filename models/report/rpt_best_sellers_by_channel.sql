{{
    config(
        materialized = 'view'
    )
}}
with
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
    from {{source('robling_dwh', 'dwh_d_prd_itm_lu')}}
),
cte_rep_color_setup as (
    select
        sty_id,
        color_id as rep_color_id,
        color_desc as rep_color_desc,
        sum(sale_amt) as sales
    from {{ref('v_fct_order_items')}}
    join cte_catalogue using (itm_key)
    where
        date between previous_day(current_date(), 'sa') - 6 and previous_day(current_date(), 'sa')
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
    date,
    channel,
    order_type,
    sty_id,
    sty_desc,
    rep_color_id,
    rep_color_desc,
    sbc_desc,
    cls_desc,
    dpt_desc,
    div_desc,
    sum(sale_amt) as sales,
    sum(sale_qty) as units
from {{ref('v_fct_order_items')}}
join cte_catalogue using (itm_key)
join cte_rep_color using (sty_id)
where
    date between previous_day(current_date(), 'sa') - 6 and previous_day(current_date(), 'sa')
group by all