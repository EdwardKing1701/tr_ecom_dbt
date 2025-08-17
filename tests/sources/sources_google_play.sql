with
cte_test as (
    select
        date as dimension,
        coalesce(sum(daily_device_installs), 0) as data
    from {{ref('cte_source_freshness_date_range')}}
    left join {{source('google_play', 'stats_installs_overview')}} using (date)
    where
        date <= current_date() - 4
    group by all
)
select
    '{{this.name}}' as test_name,
    'google_play.stats_installs_overview' as source_name,
    dimension,
    'Android downloads > 200' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 200 as passed
from cte_test
where not passed