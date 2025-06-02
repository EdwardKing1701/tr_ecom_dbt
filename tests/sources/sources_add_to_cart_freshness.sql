with
cte_test as (
    select
        coalesce(sum(users), 0) as data
    from {{ref('ga_add_to_cart_users')}}
    where
        date = current_date() - 1
)
select
    '{{this.name}}' as test_name,
    'load.ga_add_to_cart_users' as source_name,
    null as dimension,
    'Add to cart users > 5,000' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data > 5000 as passed
from cte_test
where not passed