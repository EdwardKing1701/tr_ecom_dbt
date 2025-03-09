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
    case
        when channel = 'Push' then 'Mobile Push Notifications'
        when channel = 'Social' then 'Organic Social'
        else channel
    end as channel,
    f_web_fcst_co_ord_rtl as forecast,
    f_web_bdgt_co_ord_rtl as budget,
    rcd_upd_ts as source_synced_ts
from {{source('robling_tr', 'dwh_f_web_pln_d_b')}}
natural join cte_last_updated
where
    channel <> 'ECOM Total'