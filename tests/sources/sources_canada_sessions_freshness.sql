with
cte_test as (
    select
        coalesce(sum(sessions), 0) as data
    from {{ref('ga_canada')}}
    where
        date = current_date() - 1
)
select
    '{{this.name}}' as test_name,
    'load.ga_canada' as source_name,
    null as dimension,
    'Canada sessions > 1,000' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 1000 as passed
from cte_test
where not passed