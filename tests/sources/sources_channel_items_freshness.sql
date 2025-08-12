with
cte_test as (
    select
        date as dimension,
        coalesce(sum(quantity), 0) as data
    from {{ref('cte_source_freshness_date_range')}}
    left join {{source('load', 'ga_items')}} using (date)
    group by all
)
select
    '{{this.name}}' as test_name,
    'load.ga_items' as source_name,
    dimension,
    'Channel items > 2,500' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 2500 as passed
from cte_test
where not passed