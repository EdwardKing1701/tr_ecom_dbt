{{
    config(
        materialized = 'table',
        pk = ['sty_id', 'color_id', 'price_list']
    )
}}
select
    split(style_color, '-')[0] as sty_id,
    split(style_color, '-')[1] as color_id,
    replace(source_file_name, '.xlsx', '') as price_list,
    effective_date as price_from_date,
    end_date as price_to_date,
    {# msrp, #}
    sale_price,
    coalesce(category_reg_sale, 'REG') as price_category,
    inserted_ts as source_synced_ts,
    current_timestamp() as inserted_ts
from {{source('load', 'price_lists')}}
join {{ref('stg_price_list')}} using(source_file_name)
where
    price_list_type = 'Standard'
order by price_from_date, price_list, sty_id, color_id