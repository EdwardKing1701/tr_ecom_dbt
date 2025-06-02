with
cte_test as (
    select
        date as dimension,
        coalesce(sum(sessions), 0) as data
    from {{ref('ga_channels')}}
    where
        date >= current_date() - 7
    group by all
)
select
    '{{this.name}}' as test_name,
    'load.ga_channels' as source_name,
    dimension,
    'Channel sessions > 100,000' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 100000 as passed
from cte_test
where not passed