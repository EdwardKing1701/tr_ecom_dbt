{{
    config(
        materialized = 'table',
        pk = ['color', 'price_list']
    )
}}
with
cte_price_list as (
    select
        style_color,
        replace(source_file_name, '.xlsx', '') as price_list,
        sale_price,
        coalesce(category_reg_sale, 'REG') as price_category,
        promo,
        notes,
        holds,
        source_file_name,
        coalesce(effective_date, try_to_date(regexp_replace(regexp_replace(regexp_replace(source_file_name, '^([^\\d]|\\d+%)*(\\d{1,2})\.(\\d{1,2})\.(\\d\\d).*$', '20\\4-\\2-\\3'), '-(\\d)-', '-0\\1-'), '-(\\d)$', '-0\\1'))) as effective_date,
        coalesce(price_list_type, iff(source_file_name ilike '%one site pricing%', 'standard', 'promo')) as price_list_type
    from {{source('load', 'price_lists')}}
    left join {{ref('price_list_eff_date_override')}} using(source_file_name)
    left join {{ref('price_list_type_override')}} using(source_file_name)
),
cte_standard_pricing as (
    select
        *,
        row_number() over (partition by style_color order by effective_date) as price_history_index
    from cte_price_list
    where
        price_list_type = 'standard'
)
select
    p1.style_color as color,
    p1.price_list,
    p1.effective_date,
    coalesce(dateadd('day', -1, p2.effective_date), '2199-01-01') as end_date,
    p1.sale_price,
    p1.price_category,
    p1.promo,
    p1.notes,
    p1.holds
from cte_standard_pricing p1
left join cte_standard_pricing p2
    on p1.style_color = p2.style_color
    and p1.price_history_index = p2.price_history_index - 1
order by color, effective_date