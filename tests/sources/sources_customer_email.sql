with
cte_sfcc_customer as (
    select
        sfcc_customer_id as customer_id,
        email_address
    from {{source('robling_sftp_v', 'v_dwh_f_customers')}}
    qualify row_number() over (partition by sfcc_customer_id order by sfcc_customer_id) = 1
),
cte_orders as (
    select distinct
        meas_dt,
        attr_varchar_col7 as customer_id,
        co_id as order_id
    from {{source('robling_merch', 'dv_dm_f_meas_il_b')}}
    where
        meas_dt between current_date() - 7 and current_date() - 1
        and meas_cde = 'CO_ORDERED'
        and coalesce(attr_varchar_col11, '') <> 'Facebook'
),
cte_data as (
    select
        meas_dt as dimension,
        count(*) as orders,
        count(email_address) as email_addresses,
        (orders - email_addresses) / nullifzero(orders) as data
    from cte_orders
    left join cte_sfcc_customer using (customer_id)
    group by all
)
select
    '{{this.name}}' as test_name,
    'v_dwh_f_customers' as source_name,
    dimension,
    'More than 1% of emails missing' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    data,
    null as stop_execution,
    data < 0.01 as passed
from cte_data
where not passed