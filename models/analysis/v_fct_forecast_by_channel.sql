{{
    config(
        materialized = 'view'
    )
}}
{# with
cte_last_updated as (
    select
        day_key,
        max(rcd_upd_ts) as rcd_upd_ts
    from {{source('robling_tr', 'dwh_f_web_pln_d_b')}}
    group by all
)
select
    day_key as date,
    coalesce(channel_correction, channel) as channel,
    sum(f_web_fcst_co_ord_rtl) as forecast,
    sum(f_web_bdgt_co_ord_rtl) as budget,
    max(rcd_upd_ts) as source_synced_ts
from {{source('robling_tr', 'dwh_f_web_pln_d_b')}}
natural join cte_last_updated
left join {{ref('channel_correction')}} using (channel)
where
    channel <> 'ECOM Total'
group by all #}

select
    date,
    coalesce(channel_correction, channel) as channel,
    sum(sale_amt_forecast) as forecast,
    sum(sale_amt_budget) as budget,
    null as source_synced_ts
from {{ref('rpt_forecast_source')}}
left join {{ref('channel_correction')}} using (channel)
where
    channel <> 'ECOM Total'
group by all