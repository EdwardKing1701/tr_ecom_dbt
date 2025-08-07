{{
    config(
        materialized = 'view'
    )
}}
select
    try_to_date(date) as date,
    _file,
    _line,
    _sheet_name,
    _modified,
    _fivetran_synced,
    campaign_mms_segments,
    campaign_video_send_cost,
    automated_mms_segments,
    characters,
    campaign_total,
    sends,
    message_segments,
    campaign_sms_send_cost,
    automated_carrier_fees,
    automated_sms_segments,
    campaign_carrier_fees,
    message_name,
    campaign_sms_segments,
    automated_sms_send_cost,
    image_url,
    message_type,
    campaign_video_segments,
    type,
    has_video,
    automated_mms_send_cost,
    automated_total,
    message_copy,
    campaign_mms_send_cost,
    message_id,
    total_spend
from {{source('attentive_sftp', 'campaign_detail')}}