with
cte_test as (
    select
        coalesce(sum(spend), 0) as data
    from {{ref('cte_source_freshness_date_range')}}
    left join {{ref('marketing_spend')}} using (date)
    where
        date = previous_day(current_date(), 'sa')
)
select
    '{{this.name}}' as test_name,
    'load.marketing_spend' as source_name,
    null as dimension,
    'Marketing spend > 100,000' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    (data > 100000) or (datediff('day', previous_day(current_date(), 'sa'), current_date()) < 3) as passed
from cte_test
where not passed