{{
    config(
        materialized = 'table',
        pk = ['review_id'],
        mask = {
            'analysis.pii_mask': ['email_address']
        }
    )
}}
with
cte_styles as (
    select
        style as product_id,
        style as style_style
    from {{ref('stg_items')}}
    where
        style_sku_index = 1
),
cte_skus as (
    select
        sku as product_id,
        style as sku_style
    from {{ref('stg_items')}}
),
cte_yotpo_reviews as (
    select
        review_id,
        created_ts,
        created_date,
        is_published,
        email_address,
        review_title,
        review_body,
        rating,
        is_verified_buyer,
        order_id,
        product_id,
        votes_up,
        votes_down,
        is_incentivized,
        incentive_type,
        source_synced_ts
    from {{ref('stg_yotpo_reviews')}}
)
select
    review_id,
    created_ts,
    created_date,
    is_published,
    email_address,
    review_title,
    review_body,
    rating,
    is_verified_buyer,
    coalesce(order_id, '(N/A)') as order_id,
    coalesce(style_style, sku_style) as style,
    votes_up,
    votes_down,
    is_incentivized,
    coalesce(incentive_type, '(N/A)') as incentive_type,
    source_synced_ts,
    current_timestamp() as inserted_ts
from cte_yotpo_reviews
left join cte_styles using (product_id)
left join cte_skus using (product_id)
where
    not coalesce(style_style, sku_style) is null
    and created_date < current_date()
order by created_ts, review_id