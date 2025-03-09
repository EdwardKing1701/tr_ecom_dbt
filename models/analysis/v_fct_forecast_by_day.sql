{{
    config(
        materialized = 'view'
    )
}}
with
cte_last_updated as (
    select
        day_key,
        max(rcd_upd_ts) as rcd_upd_ts
    from {{source('robling_tr', 'dwh_f_web_pln_d_b')}}
    group by all
)
select
    day_key as date,
    f_web_fcst_co_ord_qty as orders_forecast,
    f_web_fcst_co_ord_cnt as sale_qty_forecast,
    f_web_fcst_co_ord_rtl as sale_amt_forecast,
    f_web_fcst_trfc_cnt as sessions_forecast,
    f_web_bdgt_co_ord_qty as orders_budget,
    f_web_bdgt_co_ord_cnt as sale_qty_budget,
    f_web_bdgt_co_ord_rtl as sale_amt_budget,
    f_web_bdgt_trfc_cnt as sessions_budget,
    rcd_upd_ts as source_synced_ts
from {{source('robling_tr', 'dwh_f_web_pln_d_b')}}
natural join cte_last_updated
where
    channel = 'ECOM Total'