{{
    config(
        materialized = 'table',
        pk = ['category_id']
    )
}}
select
    id as category_id,
    parent_category_id,
    online,
    convert_timezone('America/Los_Angeles', creation_date)::date as created_date,
    convert_timezone('America/Los_Angeles', last_modified)::date as modified_date,
    name_default as category_name,
    page_title_default as page_title,
    c_show_in_menu as show_in_menu,
    c_menu_display as menu_display,
    c_size_chart_id as size_chart_id,
    category_id ilike '%new-arrival%' as is_new_arrival,
    convert_timezone('America/Los_Angeles', _fivetran_synced) as source_synced_ts,
    current_timestamp() as inserted_ts
from {{source('sfcc', 'category')}}
where
    catalog_id = 'tr-ecom-catalog'
    and not _fivetran_deleted
order by category_id