{{
    config(
        materialized = 'table',
        pk = ['date', 'channel']
    )
}}
select
    date,
    coalesce(channel_correction, channel) as channel,
    sum(sale_amt_forecast) as forecast,
    sum(sale_amt_budget) as budget,
    null as source_synced_ts,
    current_timestamp() as inserted_ts
from {{ref('rpt_forecast_source')}}
left join {{ref('channel_correction')}} using (channel)
where
    channel <> 'ECOM Total'
group by all