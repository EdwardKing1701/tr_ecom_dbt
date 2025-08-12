{{
    config(
        materialized = 'view'
    )
}}
select
    replace(_file, '.xlsx', '') as ugc_list,
    imagery_position,
    talent_name,
    date_added_to_site::date as date_added_to_site,
    upper(trim(sku_color, '\n')) as color,
    comment,
    convert_timezone('America/Los_Angeles', _fivetran_synced) as synced_ts
from {{source('share_point', 'ugc')}}
where
    sku_color is not null