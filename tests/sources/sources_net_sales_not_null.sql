with
cte_test as (
    select
        '{{this.name}}' as test_name,
        'load.mstr_net_sales' as source_name,
        null as dimension,
        'Net Sales not null' as description,
        null as source_synced_ts,
        null as max_data_date,
        analysis.local_time(current_timestamp()) as tested_ts,
        net_sales_retail as data,
        null as stop_execution,
        net_sales_retail is not null as passed
    from {{ref('mstr_net_sales')}}
    where
        date = current_date() - 1
)
select
    *
from cte_test
where passed = false