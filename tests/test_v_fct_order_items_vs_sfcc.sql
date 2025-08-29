with
cte_robling as (
    select
        date,
        coalesce(sum(sale_amt), 0) as sales_robling
    from {{ref('cte_source_freshness_date_range')}}
    left join {{ref('v_fct_order_items')}} using (date)
    where
        coalesce(order_type, '') <> 'Facebook'
    group by all
),
cte_sfcc as (
    select
        demand_date as date,
        coalesce(sum(sale_amt), 0) as sales_sfcc
    from {{ref('cte_source_freshness_date_range')}}
    left join {{ref('v_sfcc_orders')}} on date = demand_date
    where
        coalesce(order_type, '') <> 'Facebook'
    group by all
),
cte_test as (
    select
        date as dimension,
        abs(coalesce(sales_robling, 0) - coalesce(sales_sfcc, 0)) as data
    from cte_robling
    natural full join cte_sfcc
)
select
    '{{this.name}}' as test_name,
    'analysis.v_fct_order_items' as source_name,
    null as dimension,
    'Robling sales match SFCC' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data < 2000 as passed
from cte_test
where not passed