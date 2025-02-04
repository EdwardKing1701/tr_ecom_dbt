{{
    config(
        materialized = 'table',
        pk = ['date', 'channel']
    )
}}
select
    day_key as date,
    case
        when channel = 'Push' then 'Mobile Push Notifications'
        when channel = 'Social' then 'Organic Social'
        else channel
    end as channel,
    f_web_fcst_co_ord_rtl as forecast,
    f_web_bdgt_co_ord_rtl as budget,
    rcd_upd_ts as source_synced_ts,
    current_timestamp() as inserted_ts
from robling_prd_db.dw_dwh_tr.dwh_f_web_pln_d_b
where
    channel <> 'ECOM Total'
order by 1,2