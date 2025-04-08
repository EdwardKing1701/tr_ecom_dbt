{{
    config(
        materialized = 'table',
        pk = ['style', 'color', 'price_list'],
        enabled = false
    )
}}
select
    split(style_color, '-')[0]::varchar as style,
    split(style_color, '-')[1]::varchar as color,
    replace(source_file_name, '.xlsx', '') as price_list,
    effective_date as price_from_date,
    end_date as price_to_date,
    sale_price,
    coalesce(category_reg_sale, 'REG') as price_category,
    notes,
    holds,
    inserted_ts as source_synced_ts,
    current_timestamp() as inserted_ts
from {{source('load', 'price_lists')}}
{# join {{ref('stg_price_list')}} using(source_file_name) #}
where
    price_list_type = 'Standard'
order by price_from_date, price_list, style, color