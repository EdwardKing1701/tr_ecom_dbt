{{
    config(
        materialized = 'table',
        pk = ['date']
    )
}}
with
cte_generate_days as (
    select dateadd('d', seq4(), date_from_parts(2011, 1, 30)) as date
    from table(generator(rowcount=>50000))
),
cte_setup_year as (
    select
        year_id,
        dateadd('d', [-1, -2, -3, 3, 2, 1, 0][dayofweek(date_from_parts(year_id, 1, 31))] + 1, date_from_parts(year_id, 1, 31)) as year_start_date,
        dateadd('d', [-1, -2, -3, 3, 2, 1, 0][dayofweek(date_from_parts(year_id + 1, 1, 31))], date_from_parts(year_id + 1, 1, 31)) as year_end_date,
        datediff('day', year_start_date, year_end_date) + 1 as total_days_in_year
    from (select distinct year(date) as year_id from cte_generate_days)
),
cte_setup_date as (
    select
        *,
        row_number() over (partition by year_id order by date) as day_in_year,
        ceil(day_in_year / 7) as week_number,
        (year_id::text || lpad(week_number, 2, '0'))::int as week_id,
        least(4, ceil(week_number / 13)) as quarter_number,
        (year_id::text || lpad(quarter_number, 2, '0'))::int as quarter_id,
        case
            when week_number = 53 then 3
            when mod(week_number, 13) = 0 then 3
            when mod(week_number, 13) between 10 and 12 then 3
            when mod(week_number, 13) between 5 and 9 then 2
            when mod(week_number, 13) between 1 and 4 then 1
        end as month_in_quarter,
        ((quarter_number - 1) * 3) + month_in_quarter as month_number,
        (year_id::text || lpad(month_number, 2, '0'))::int as month_id
    from cte_generate_days
    join cte_setup_year
    where
        date >= year_start_date
        and date <= year_end_date
        and year_id <= year(current_date()) + 3
),
cte_year_ly as (
    select
        year_id + 1 as year_id,
        datediff('day', year_start_date, year_end_date) + 1 as total_days_in_year_ly
    from cte_setup_year
),
cte_year_lly as (
    select
        year_id + 2 as year_id,
        datediff('day', year_start_date, year_end_date) + 1 as total_days_in_year_lly
    from cte_setup_year
),
cte_date as (
    select
        date,
        week_id,
        month_id,
        quarter_id,
        year_id,
        dateadd('day', -364, date) as date_ly_shifted,
        dateadd('day', -7, date) as date_lw,
        dayname(date) as day_name,
        rank() over (partition by week_id order by date) as day_in_week,
        rank() over (partition by month_id order by date) as day_in_month,
        rank() over (partition by quarter_id order by date) as day_in_quarter,
        day_in_year
    from cte_setup_date
),
cte_week as (
    select
        week_id,
        week_number,
        'WK' || lpad(week_number, 2, '0') as week_short_name,
        'WEEK ' || lpad(week_number, 2, '0') as week_name,
        year_id || ' WEEK ' || week_number as week_long_name,
        min(date) as week_start_date,
        max(date) as week_end_date
    from cte_setup_date
    group by all
),
cte_month_name as (
    select
        month_id,
        to_varchar(date,'MMMM') as month_name
    from cte_setup_date
    qualify row_number() over (partition by month_id order by date) = 15
),
cte_month as (
    select
        month_id,
        month_number,
        month_name,
        'P' || lpad(month_number, 2, '0') || '-' || upper(left(month_name, 3)) as month_short_name,
        year_id || ' PERIOD ' || month_number || ' - ' || upper(left(month_name, 3)) as month_long_name,
        min(date) as month_start_date,
        max(date) as month_end_date,
        count(*) as total_days_in_month
    from cte_setup_date
    natural join cte_month_name
    group by all
),
cte_quarter as (
    select
        quarter_id,
        quarter_number,
        year_id || 'Q' || quarter_number as quarter_name,
        'Q' || quarter_number as quarter_short_name,
        year_id || ' QUARTER ' || quarter_number as quarter_long_name,
        min(date) as quarter_start_date,
        max(date) as quarter_end_date,
        count(*) as total_days_in_quarter
    from cte_setup_date
    group by all
),
cte_year as (
    select
        year_id,
        year_id as year_name,
        min(date) as year_start_date,
        max(date) as year_end_date,
        count(*) as total_days_in_year
    from cte_setup_date
    group by all
),
cte_all as (
    select
        date,
        dateadd('day', -1 * total_days_in_year_ly, date) as date_ly,
        dateadd('day', -1 * (total_days_in_year_ly + total_days_in_year_lly), date) as date_lly,
        date_ly_shifted,
        date_lw,
        dateadd('day', -1 * total_days_in_month, date) as date_lm,
        dateadd('day', -1 * total_days_in_quarter, date) as date_lq,
        day_name,
        rank() over (partition by week_id order by date) as day_in_week,
        rank() over (partition by month_id order by date) as day_in_month,
        rank() over (partition by quarter_id order by date) as day_in_quarter,
        ceil(day_in_month / 7) as week_in_month,
        day_in_year,
        week_id,
        week_number,
        week_short_name,
        week_name,
        week_long_name,
        week_start_date,
        week_end_date,
        month_id,
        month_number,
        month_name,
        month_short_name,
        month_long_name,
        month_start_date,
        month_end_date,
        total_days_in_month,
        quarter_id,
        quarter_number,
        quarter_name,
        quarter_short_name,
        quarter_long_name,
        quarter_start_date,
        quarter_end_date,
        total_days_in_quarter,
        year_id,
        year_name,
        year_start_date,
        year_end_date,
        total_days_in_year,
        total_days_in_year_ly,
        total_days_in_year_lly
    from cte_date
    natural join cte_week
    natural join cte_month
    natural join cte_quarter
    natural join cte_year
    natural join cte_year_ly
    natural join cte_year_lly
)
select
    *
from cte_all
where
    year_id >= 2019
    and year_id <= (select year_id + 1 from cte_all where date = current_date() - 1)