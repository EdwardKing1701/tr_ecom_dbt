with
cte_test as (
    select
        '{{this.name}}' as test_name,
        'load.ga_items' as source_name,
        null as dimension,
        'Channel items > 2,500' as description,
        null as source_synced_ts,
        null as max_data_date,
        analysis.local_time(current_timestamp()) as tested_ts,
        sum(quantity) as data,
        null as stop_execution,
        data > 2500 as passed
    from {{ref('ga_items')}}
    where
        date = current_date() - 1
    group by all
)
select
    *
from cte_test
where passed = false