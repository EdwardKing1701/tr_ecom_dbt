{{
    config(
        materialized = 'view'
    )
}}
with
cte_orders as (
    select distinct
        order_id,
        demand_date as date
    from {{ref('v_sfcc_orders')}}
),
cte_payment_instrument as (
    select
        payment_instrument_id,
        payment_method,
        c_paypal_origination,
        payment_cardcard_type
    from {{source('sfcc', 'payment_instrument')}}
),
cte_order_payment as (
    select
        order_id,
        payment_instrument_id,
        payment_transaction_amount as payment_amount
    from {{source('sfcc', 'order_payment_instrument')}}
),
cte_all_sfcc as (
    select
        *
    from cte_orders
    join cte_order_payment using (order_id)
    join cte_payment_instrument using (payment_instrument_id)
),
cte_payments as (
    select
        date,
        case
            when payment_method in ('FLOW_HOSTED_CHECKOUT', 'GLOBALE') then 'International'
            when payment_method = 'AMAZON_PAYMENTS' then 'Amazon'
            when payment_method = 'AFTERPAY_PBI' then 'Afterpay'
            when payment_method = 'DW_APPLE_PAY' then 'Apple Pay'
            when payment_method = 'CREDIT_CARD' then initcap(replace(payment_cardcard_type, '_', ' '))
            {# when c_paypal_origination is not null then payment_method || '-' || c_paypal_origination #}
            else initcap(replace(payment_method, '_', ' '))
        end as payment_method_name,
        count(distinct order_id) as payments,
        sum(payment_amount) as payment_amount
    from cte_all_sfcc
    join {{ref('dim_date')}} using (date)
    where
        date < current_date()
    group by all
)
select
    date,
    payment_method_name,
    payment_method_group,
    payments,
    payment_amount
from cte_payments
left join {{ref('payment_method_group')}} using (payment_method_name)