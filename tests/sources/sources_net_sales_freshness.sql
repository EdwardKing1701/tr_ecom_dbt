with
cte_test as (
    select
        coalesce(sum(net_sales_retail), 0) as data
    from {{ref('cte_source_freshness_date_range')}}
    left join {{ref('mstr_net_sales')}} using (date)
)
select
    '{{this.name}}' as test_name,
    'load.mstr_net_sales' as source_name,
    null as dimension,
    'Net Sales not null' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data <> 0 as passed
from cte_test
where not passed