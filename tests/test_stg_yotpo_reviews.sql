with
cte_test as (
    select
        date as dimension,
        coalesce(count(distinct review_id), 0) as data
    from {{ref('cte_source_freshness_date_range')}}
    left join {{ref('stg_yotpo_reviews')}} on date = created_date
    group by all
)
select
    '{{this.name}}' as test_name,
    'stg.stg_yotpo_reviews' as source_name,
    dimension,
    'New reviews > 0' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 0 as passed
from cte_test
where not passed