{{
    config(
        materialized = 'table',
        tags = ['daily']
    )
}}
select
    '{{this.name}}' as test_name,
    null as source_name,
    null as dimension,
    'Validation freshness' as description,
    null as source_synced_ts,
    null as max_data_date,
    analysis.local_time(current_timestamp()) as tested_ts,
    null as data,
    null as stop_execution,
    false as passed