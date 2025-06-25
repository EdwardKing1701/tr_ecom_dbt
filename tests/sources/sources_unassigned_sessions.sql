with
cte_test as (
    select (select sum(sessions) from {{ref('ga_channels')}} where date = current_date() - 1 and channel = 'Unassigned') / (select sum(sessions) from {{ref('ga_channels')}} where date = current_date() - 1) as data
)
select
    '{{this.name}}' as test_name,
    'load.ga_channels' as source_name,
    null as dimension,
    '% Unassigned < 10%' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data < 0.10 as passed
from cte_test
where not passed