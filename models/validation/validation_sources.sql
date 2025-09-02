{{
    config(
        materialized = 'view'
    )
}}

{%- if execute -%}
{%- set l = run_query("select table_name from information_schema.tables where table_schema = 'VALIDATION' and table_name ilike 'test%' and table_type = 'BASE TABLE'").columns[0].values() -%}
{%- for t in l -%}
select
    test_name,
    source_name,
    dimension::varchar as dimension,
    description,
    source_synced_ts,
    max_data_date,
    tested_ts,
    data::variant as data,
    stop_execution::variant as stop_execution,
    passed
from {{database}}.validation.{{t}}
{% if not loop.last %}union all{% endif %}
{% endfor -%}
{%- endif -%}