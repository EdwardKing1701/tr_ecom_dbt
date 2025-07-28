with
cte_test as (
    select
        coalesce(sum(sale_amt_forecast), 0) as data
    from {{ref('cte_source_freshness_date_range')}}
    left join {{ref('fct_forecast_by_day')}} using (date)
)
select
    '{{this.name}}' as test_name,
    'analysis.v_forecast_by_day' as source_name,
    null as dimension,
    'Forecast > 100,000' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 100000 as passed
from cte_test
where not passed