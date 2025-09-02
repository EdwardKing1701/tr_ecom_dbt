{{
    config(
        materialized = 'table',
        pk = ['date']
    )
}}
with
cte_net_sales_budget as ( --added to the first day of the month as the net sales budget is not reported daily
    select
        date,
        net_sale_amt_budget,
        net_sale_cost_budget,
        shipping_budget
    from {{ref('ecom_demand_budget')}}
    join {{ref('stg_date')}} using (month_id)
    where
        day_in_month = 1
)
select
    date,
    orders_forecast,
    sale_qty_forecast,
    sale_amt_forecast,
    sessions_forecast,
    orders_budget,
    sale_qty_budget,
    sale_amt_budget,
    sessions_budget,
    net_sale_amt_budget,
    net_sale_cost_budget,
    shipping_budget,
    null as source_synced_ts,
    current_timestamp() as inserted_ts
from {{ref('stg_forecast_source')}}
left join cte_net_sales_budget using (date)
where
    channel = 'ECOM Total'
order by date