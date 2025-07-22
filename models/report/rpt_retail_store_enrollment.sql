{{
    config(
        materialized = 'view'
    )
}}
With TTL_PURCHASE_TRANSACTIONS as (select 
WEEK_ID as Week,
store_no as Store_Number,
count( Distinct TRANSACTION_ID ) as TTL_PURCHASE_TRANSACTIONS,


from {{source('load', 'retail_loyalty_store_id')}} 
 left join {{ref('dim_date')}} AS d
ON d.DATE = TO_DATE(tdate)

group by WEEK_ID , store_no),

RETURNING_MEMBER_TRANSACTIONS as (select 
WEEK_ID as Week,
store_no as Store_Number,
COUNT(DISTINCT CASE WHEN (new_returning = 'Returning') THEN transaction_id ELSE NULL END) AS RETURNING_MEMBER_TRANSACTIONS

from {{source('load', 'retail_loyalty_store_id')}}
 left join {{ref('dim_date')}} AS d
ON d.DATE = TO_DATE(tdate)

group by all),

TTL_ELIGIBLE_TRANSACTIONS as (
select 
WEEK_ID as Week,
store_no as Store_Number,
COUNT(DISTINCT CASE WHEN (LOYALTY_TRANSACTION_TYPE = 'Non_loyalty_transaction' OR new_returning = 'New') THEN transaction_id ELSE NULL END) AS TTL_ELIGIBLE_TRANSACTIONS


from {{source('load', 'retail_loyalty_store_id')}} 
 left join {{ref('dim_date')}} AS d
ON d.DATE = TO_DATE(tdate)
group by all),

District as (
Select
STORE_NO,
Max(cast(district_code as NUMBER)) as district,

from {{source('load', 'retail_loyalty_store_id')}} 
group by all
order by store_no),
 enrollments as (WITH weekly_enrollments AS (
    SELECT
        d.WEEK_ID AS week,
        c.LOYALTY_OPTIN_STORE_NO AS store_number,
        COUNT(DISTINCT 
            CASE 
                WHEN c.EMAIL_ADDRESS IS NULL 
                THEN CONCAT(YEAR(c.loyalty_optin_dt), c.customer_id) 
                ELSE c.EMAIL_ADDRESS 
            END
        ) AS enrollment_count
    FROM {{source('load', 'retail_loyalty_manual_upload')}} c
    LEFT JOIN {{ref('dim_date')}} d 
        ON d.DATE = TO_DATE(c.loyalty_optin_dt)
    WHERE 
         c.LOYALTY_OPTIN_STORE_NO NOT IN (10777, 9999)
    GROUP BY
        d.WEEK_ID,
        c.LOYALTY_OPTIN_STORE_NO
),

with_wow AS (
    SELECT
        curr.week,
        curr.store_number,
        curr.enrollment_count AS current_week_enrollment,
        prev.enrollment_count AS prior_week_enrollment,
        ROUND(
            CASE 
                WHEN prev.enrollment_count = 0 THEN NULL
                ELSE ((curr.enrollment_count - prev.enrollment_count) / prev.enrollment_count) * 100
            END, 2
        ) AS wow_change_percent
    FROM weekly_enrollments curr
    LEFT JOIN weekly_enrollments prev
        ON curr.store_number = prev.store_number
        AND curr.week = prev.week + 1  -- assumes sequential week IDs
)

SELECT * FROM with_wow
ORDER BY store_number, week)



Select 
a.Week,
d.district,
a.Store_Number,
TTL_PURCHASE_TRANSACTIONS,
RETURNING_MEMBER_TRANSACTIONS,
TTL_ELIGIBLE_TRANSACTIONS,
current_week_enrollment as Enrollment_count,
(enrollment_COUNT/TTL_ELIGIBLE_TRANSACTIONS) as ENROLLMENT_RATE,
(wow_change_percent/100) as WOW

From 
TTL_PURCHASE_TRANSACTIONS a
Left JOIN RETURNING_MEMBER_TRANSACTIONS b  on a.week = b.week and a.Store_Number=b.Store_Number 
left join TTL_ELIGIBLE_TRANSACTIONS c on a.week = c.week and a.Store_Number=c.Store_Number 
left join enrollments e on a.week = e.week and a.store_number = e.store_number
left join {{source('load', 'retail_district_code')}} d on a.store_number = d.store_number
where a.week >= 202501 

group by all