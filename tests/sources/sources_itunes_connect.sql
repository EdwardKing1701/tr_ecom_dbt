with
cte_test as (
    select
        date as dimension,
        coalesce(sum(counts), 0) as data
    from {{ref('cte_source_freshness_date_range')}}
    left join {{source('itunes_connect', 'app_store_download_standard_daily')}} using (date)
    group by all
)
select
    '{{this.name}}' as test_name,
    'itunes_connect.app_store_download_standard_daily' as source_name,
    dimension,
    'iOS downloads > 5,000' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 5000 as passed
from cte_test
where not passed