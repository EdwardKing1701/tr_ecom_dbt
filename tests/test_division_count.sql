{{
    config(
        severity = 'warn'
    )
}}
with
cte_test as (
    select
        count(distinct division) as data
    from {{ref('stg_items')}}
)
select
    '{{this.name}}' as test_name,
    'stg.stg_items' as source_name,
    null as dimension,
    'More than 6 divisions exist' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data <= 6 as passed
from cte_test
where not passed