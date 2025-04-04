{{
    config(
        materialized = 'table',
        pk = ['source_file_name']
    )
}}
with
cte_price_lists as (
    select
        source_file_name,
        coalesce(price_list_type_override,
            case
                when source_file_name ilike '%One Site Pricing%' then
                    'Standard'
                else
                    'Promo'
            end
        ) as price_list_type,
        price_list_type_override,
        try_to_date(regexp_replace(regexp_replace(regexp_replace(source_file_name, '^([^\\d]|\\d+%)*(\\d{1,2})\.(\\d{1,2})\.(\\d\\d).*$', '20\\4-\\2-\\3'), '-(\\d)-', '-0\\1-'), '-(\\d)$', '-0\\1')) as effective_date,
        count(distinct style_color) as item_count
    from {{source('load', 'price_lists')}}
    left join {{ref('price_file_type')}} using(source_file_name)
    group by all
),
cte_end_date as (
    select
        source_file_name as next_source_file_name,
        dateadd('day', -1, effective_date) as end_date
    from cte_price_lists
    where
        price_list_type = 'Standard'
    union all
    select
        '(N/A)' as next_price_list,
        '2199-01-01' as end_date
)
select
    source_file_name,
    next_source_file_name,
    price_list_type,
    effective_date,
    end_date,
    item_count
from cte_price_lists
left join cte_end_date
where
    end_date >= effective_date
qualify row_number() over (partition by source_file_name order by end_date) = 1
order by effective_date, price_list_type desc