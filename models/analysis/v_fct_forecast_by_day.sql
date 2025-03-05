{{
    config(
        materialized = 'view'
    )
}}
select
    day_key as date,
    f_web_fcst_co_ord_cnt as orders_forecast,
    f_web_fcst_co_ord_qty as sale_qty_forecast,
    f_web_fcst_co_ord_rtl as sale_amt_forecast,
    f_web_fcst_trfc_cnt as sessions_forecast,
    f_web_bdgt_co_ord_cnt as orders_budget,
    f_web_bdgt_co_ord_qty as sale_qty_budget,
    f_web_bdgt_co_ord_rtl as sale_amt_budget,
    f_web_bdgt_trfc_cnt as sessions_budget,
    rcd_upd_ts as source_synced_ts
from {{source('robling_tr', 'dwh_f_web_pln_d_b')}}
where
    channel = 'ECOM Total'