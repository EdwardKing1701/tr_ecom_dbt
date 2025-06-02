with
cte_test as (
    select
        coalesce(sum(sessions), 0) as data
    from {{ref('ga_international')}}
    where
        date = current_date() - 1
)
select
    '{{this.name}}' as test_name,
    'load.ga_international' as source_name,
    null as dimension,
    'International sessions > 20,000' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 20000 as passed
from cte_test
where not passed