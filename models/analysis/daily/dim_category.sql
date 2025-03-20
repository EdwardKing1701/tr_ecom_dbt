{{
    config(
        materialized = 'table',
        pk = ['category_id']
    )
}}
with
cte_hierarchy as (
    select
        id,
        substr(sys_connect_by_path(id, ' > '), 4) as category_id_path,
        len(category_id_path) - len(replace(category_id_path, '>', '')) as category_level,
        iff(category_level > 1, split_part(category_id_path, ' > ', 2), null) as top_category_id,
    from {{source('sfcc', 'category')}}
    where
        catalog_id = 'tr-ecom-catalog'
    start with id = 'root'
    connect by parent_category_id = prior id
)
select
    id as category_id,
    parent_category_id,
    online,
    convert_timezone('America/Los_Angeles', creation_date)::date as created_date,
    convert_timezone('America/Los_Angeles', last_modified)::date as modified_date,
    category_id_path,
    category_level,
    top_category_id,
    name_default as category_name,
    page_title_default as page_title,
    c_show_in_menu as show_in_menu,
    c_menu_display as menu_display,
    c_size_chart_id as size_chart_id,
    category_id ilike '%new-arrival%' as is_new_arrival,
    convert_timezone('America/Los_Angeles', _fivetran_synced) as source_synced_ts,
    current_timestamp() as inserted_ts
from {{source('sfcc', 'category')}}
left join cte_hierarchy using(id)
where
    catalog_id = 'tr-ecom-catalog'
    and not _fivetran_deleted
order by category_id