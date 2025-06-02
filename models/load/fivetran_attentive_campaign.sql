{{
    config(
        materialized = 'view'
    )
}}
select
    try_to_date(date) as date,
    message_id,
    message_name,
    message_segments,
    sends,
    sms_send_cost,
    carrier_fees,
    total,
    _file,
    _sheet_name,
    _line,
    _modified,
    _fivetran_synced
from {{source('attentive_sftp', 'campaign')}}
where
    date <> 'Total'