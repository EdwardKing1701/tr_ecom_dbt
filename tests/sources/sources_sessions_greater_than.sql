with
cte_test as (
    select
        '{{this.name}}' as test_name,
        'load.ga_sessions' as source_name,
        null as dimension,
        'Sessions > 100,000' as description,
        null as source_synced_ts,
        null as max_data_date,
        analysis.local_time(current_timestamp()) as tested_ts,
        sum(sessions) as data,
        null as stop_execution,
        data > 100000 as passed
    from {{ref('ga_sessions')}}
    where
        date = current_date() - 1
    group by all
)
select
    *
from cte_test
where passed = false