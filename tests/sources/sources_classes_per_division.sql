with
cte_test as (
    select
        division as dimension,
        count(distinct class) as data
    from {{ref('v_dim_item')}}
    group by all
)
select
    '{{this.name}}' as test_name,
    'analysis.v_dim_item' as source_name,
    dimension,
    'More than 50 classes in a division' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data <= 50 as passed
from cte_test
where not passed