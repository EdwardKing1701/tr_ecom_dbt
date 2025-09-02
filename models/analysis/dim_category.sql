{{
    config(
        materialized = 'table',
        pk = ['category_id']
    )
}}
select
    id as category_id,
    coalesce(parent_category_id, '(N/A)') as parent_category_id,
    substr(sys_connect_by_path(category_id, ' > '), 4) as category_id_path,
    coalesce(nullif(split_part(category_id_path, ' > ', 2), ''), '(N/A)') as top_category_id,
    len(category_id_path) - len(replace(category_id_path, '>', '')) as category_level,
    online,
    convert_timezone('America/Los_Angeles', creation_date)::date as created_date,
    convert_timezone('America/Los_Angeles', last_modified)::date as modified_date,
    coalesce(name_default, '(N/A)') as category_name,
    coalesce(page_title_default, '(N/A)') as page_title,
    coalesce(c_show_in_menu, false) as show_in_menu,
    coalesce(c_menu_display, '(N/A)') as menu_display,
    coalesce(c_size_chart_id, '(N/A)') as size_chart_id,
    category_id ilike '%new-arrival%' as is_new_arrival,
    convert_timezone('America/Los_Angeles', _fivetran_synced) as source_synced_ts,
    current_timestamp() as inserted_ts
from {{source('sfcc', 'category')}}
where
    catalog_id = 'tr-ecom-catalog'
    and not _fivetran_deleted
start with id = 'root'
connect by parent_category_id = prior id
order by category_id_path