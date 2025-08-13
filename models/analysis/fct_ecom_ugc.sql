{{
    config(
        materialized = 'table',
        pk = ['ugc_list', 'color']
    )
}}
select
    ugc_list,
    color,
    date_added_to_site,
    imagery_position,
    talent_name,
    comment,
    synced_ts,
    current_timestamp() as inserted_ts
from {{ref('src_share_point_ugc')}}
qualify row_number() over (partition by ugc_list, color order by date_added_to_site) = 1
order by ugc_list, color