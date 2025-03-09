with
cte_test as (
    select
        coalesce(sum(sale_amt), 0) as data
    from {{ref('v_fct_order_items')}}
    where
        date = current_date() - 1
)
select
    '{{this.name}}' as test_name,
    'analysis.v_fct_order_items' as source_name,
    null as dimension,
    'Sales > 100,000' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 100000 as passed
from cte_test
where not passed