{{
    config(
        materialized = 'view'
    )
}}
with
cte_orders as (
    select
        id as order_id,
        case
            when id like 'FL%' then
                creation_date::date
            else
                convert_timezone('America/Los_Angeles', creation_date)::date
        end as xfrm_date,
        creation_date
from {{source('sfcc', 'orders_history')}}
qualify row_number() over (partition by id order by _fivetran_synced desc) = 1
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
    join cte_order_payment using(order_id)
    join cte_payment_instrument using(payment_instrument_id)
)
select
    week_end_date as date,
    time_period,
    case
        when payment_method = 'CREDIT_CARD' then upper(payment_cardcard_type)
        when c_paypal_origination is not null then payment_method || '-' || c_paypal_origination
        else payment_method
    end as payment_method_name,
    count(distinct order_id) as payments,
    sum(payment_amount) as payment_amount
from cte_all_sfcc
join {{ref('date_xfrm')}} using(xfrm_date)
join {{ref('dim_date')}} using(date)
where
    to_date_type = 'TODAY'
    and time_period in ('TY', 'LY')
    and year_id >= (select year_id - 1 from {{ref('dim_date')}} where date = current_date() - 1)
    and year_id <= (select year_id from {{ref('dim_date')}} where date = current_date() - 1)
    and date <= previous_day(current_date(), 'sa')
group by all