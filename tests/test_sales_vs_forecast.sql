with
cte_test as (
    select coalesce((
        select
            sum(sale_amt)
        from {{ref('v_fct_order_items')}}
        where
            date = current_date() - 1
    ) / (
        select
            sum(sale_amt_forecast)
        from {{ref('fct_forecast_by_day')}}
        where
            date = current_date() - 1
    ) - 1, 1) as data
)
select
    '{{this.name}}' as test_name,
    'analysis.v_fct_order_items' as source_name,
    null as dimension,
    'Sales +/- 50% to Forecast' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    abs(data) < 0.5 as passed
from cte_test
where not passed