{{
    config(
        materialized = 'view'
    )
}}
select
    customer_id::varchar as lexer_customer_id,
    nullif(email, '') as email_address,
    nullif(user_id, '') as yotopo_user_id,
    nullif(gender, '') as gender,
    nullif(inferred_gender, '') as inferred_gender,
    nullif(generation, '') as generation,
    is_a_customer,
    first_order_date,
    record as lexer_record,
    nullif(listrak_record, '')::boolean as listrak_record,
    nullif(communication_opt_in, '')::boolean as communication_opt_in,
    nullif(opt_in, '') as listrak_opt_in,
    nullif(opt_in_status, '') as listrak_opt_in_status,
    try_to_timestamp_ntz(subscribe_date___master_list) as listrak_subscribe_date_master_list,
    try_to_timestamp_ntz(unsubscribe_date___master_list) as listrak_unsubscribe_date_master_list,
    nullif(sms_opt_in_status, '') as sms_opt_in_status,
    try_to_timestamp_ntz(sms_subscribe_date) as sms_subscribe_date,
    try_to_timestamp_ntz(sms_unsubscribe_date) as sms_unsubscribe_date,
    try_to_timestamp_ntz(app_first_open_date) as app_first_open_date,
    nullif(push_opt_in, '') as push_opt_in
from {{source('load', 'lexer_members')}}