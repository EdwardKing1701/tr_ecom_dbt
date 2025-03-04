with
cte_test as (
    select
        '{{this.name}}' as test_name,
        'analysis.v_fct_order_items' as source_name,
        null as dimension,
        'Sales > 0' as description,
        null as source_synced_ts,
        null as max_data_date,
        analysis.local_time(current_timestamp()) as tested_ts,
        sum(sale_amt) as data,
        null as stop_execution,
        data > 0 as passed
    from {{ref('v_fct_order_items')}}
    where
        date = current_date() - 1
    group by all
)
select
    *
from cte_test
where passed = false