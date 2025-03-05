{{
    config(
        materialized = 'table',
        pk = ['date']
    )
}}
select
    date,
    net_sales_units as net_sale_qty,
    net_sales_retail - gross_margin as net_sale_cost,
    net_sales_retail as net_sale_amt,
    customer_returns_units as return_qty,
    customer_returns_cost as return_cost,
    customer_returns_retail as return_amt,
    gross_sales_units as gross_sale_qty,
    gross_sales_retail - sales_gross_margin as gross_sale_cost,
    gross_sales_retail as gross_sale_amt,
    current_timestamp() as inserted_ts
from {{ref('mstr_net_sales')}}
where
    date < current_date()
order by date