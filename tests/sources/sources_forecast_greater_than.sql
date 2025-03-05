with
cte_test as (
    select
        '{{this.name}}' as test_name,
        'analysis.v_forecast_by_day' as source_name,
        null as dimension,
        'Forecast > 100,000' as description,
        null as source_synced_ts,
        null as max_data_date,
        analysis.local_time(current_timestamp()) as tested_ts,
        sum(sale_amt_forecast) as data,
        null as stop_execution,
        data > 100000 as passed
    from {{ref('v_fct_forecast_by_day')}}
    where
        date = current_date() - 1
    group by all
)
select
    *
from cte_test
where passed = false