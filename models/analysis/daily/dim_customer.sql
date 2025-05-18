{{
    config(
        materialized = 'table',
        pk = ['customer_id']
    )
}}
select
    trim(lower(sfcc_customer_id)) as customer_id,
    analysis.email_address(email_address) as email_address,
    current_timestamp() as inserted_ts
from {{source('robling_sftp_v', 'v_dwh_f_customers')}}
where
    sfcc_customer_id is not null
    and email_address is not null
qualify row_number() over (partition by trim(lower(sfcc_customer_id)) order by email_address) = 1
order by customer_id