with
cte_test as (
    select
        coalesce(sum(shipping), 0) as data
    from {{ref('cte_source_freshness_date_range')}}
    left join {{ref('v_fct_orders')}} using (date)
)
select
    '{{this.name}}' as test_name,
    'analysis.v_fct_orders' as source_name,
    null as dimension,
    'Shipping > 100' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 100 as passed
from cte_test
where not passed