{{
    config(
        materialized = 'table',
        pk = ['email_address']
    )
}}
with
cte_email_address as (
    select
        trim(lower(sfcc_customer_id)) as customer_id,
        analysis.email_address(email_address) as email_address,
        current_timestamp() as inserted_ts
    from {{source('robling_sftp_v', 'v_dwh_f_customers')}}
    where
        sfcc_customer_id is not null
        and email_address is not null
    qualify row_number() over (partition by trim(lower(sfcc_customer_id)) order by email_address) = 1
),
cte_orders as (
    select
        meas_dt as date,
        trim(lower(attr_varchar_col7)) as customer_id,
        co_id as order_id,
        attr_varchar_col11 as order_type,
        f_meas_rtl as sale_amt,
        f_meas_qty as sale_qty,
        f_meas_cst as sale_cost
    from {{source('robling_merch', 'dv_dm_f_meas_il_b')}}
    where
        meas_cde = 'CO_ORDERED'
        and meas_dt < current_date()
)
select
    email_address,
    array_agg(distinct customer_id) as customer_ids,
    min(date) as first_order_date,
    max(date) as last_order_date,
    count(distinct order_id) as orders_ltd,
    sum(sale_cost) as sale_cost_ltd,
    sum(sale_qty) as sale_qty_ltd,
    sum(sale_amt) as sale_amt_ltd,
    current_timestamp() as inserted_ts
from cte_orders
join cte_email_address using (customer_id)
group by all
order by email_address