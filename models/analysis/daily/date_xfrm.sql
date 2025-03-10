{{
    config(materialized = 'table')
}}
with
cte_calendar as (
    select *
    from {{ref('dim_date')}}
    where
        date in (current_date(), current_date() - 7, current_date() - 364)
),
wow_tw_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'TW' as time_period,
		'WoW' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date = a.date
),
wow_lw_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'LW' as time_period,
		'WoW' as comparison_type,
		b.date_lw as xfrm_date
	from wow_tw_today a
	join cte_calendar b
		on a.xfrm_date = b.date
),
wow_prior_four_weeks_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'Prior 4 Weeks' as time_period,
		'WoW' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date >= dateadd('d', -28, a.date)
		and b.date <= dateadd('d', -7, a.date)
		and dayname(b.date) = dayname(a.date)
),
wow_tw_wtd as (
	select
		a.date,
		'WTD' as to_date_type,
		'TW' as time_period,
		'WoW' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date >= a.week_start_date
		and b.date <= a.date
),
wow_lw_wtd as (
	select
		a.date,
		'WTD' as to_date_type,
		'LW' as time_period,
		'WoW' as comparison_type,
		b.date_lw as xfrm_date
	from wow_tw_wtd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
pop_tp_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'TM' as time_period,
		'MoM' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date = a.date
),
pop_lp_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'LM' as time_period,
		'MoM' as comparison_type,
		b.date_lm as xfrm_date
	from pop_tp_today a
	join cte_calendar b
		on a.xfrm_date = b.date
),
pop_tp_ptd as (
	select
		a.date,
		'MTD' as to_date_type,
		'TM' as time_period,
		'MoM' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date >= a.month_start_date
		and b.date <= a.date
),
pop_lp_ptd as (
	select
		a.date,
		'MTD' as to_date_type,
		'LM' as time_period,
		'MoM' as comparison_type,
		b.date_lm as xfrm_date
	from pop_tp_ptd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
qoq_tq_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'TQ' as time_period,
		'QoQ' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date = a.date
),
qoq_lq_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'LQ' as time_period,
		'QoQ' as comparison_type,
		b.date_lq as xfrm_date
	from qoq_tq_today a
	join cte_calendar b
		on a.xfrm_date = b.date
),
qoq_tq_qtd as (
	select
		a.date,
		'QTD' as to_date_type,
		'TQ' as time_period,
		'QoQ' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date >= a.quarter_start_date
		and b.date <= a.date
),
qoq_lq_qtd as (
	select
		a.date,
		'QTD' as to_date_type,
		'LQ' as time_period,
		'QoQ' as comparison_type,
		b.date_lq as xfrm_date
	from qoq_tq_qtd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_ty_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'TY' as time_period,
		'YoY' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date = a.date
),
yoy_ly_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'LY' as time_period,
		'YoY' as comparison_type,
		b.date_ly as xfrm_date
	from yoy_ty_today a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_lly_today as (
	select
		a.date,
		'TODAY' as to_date_type,
		'LLY' as time_period,
		'YoY' as comparison_type,
		b.date_lly as xfrm_date
	from yoy_ty_today a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_ty_wtd as (
	select
		a.date,
		'WTD' as to_date_type,
		'TY' as time_period,
		'YoY' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date >= a.week_start_date
		and b.date <= a.date
),
yoy_ly_wtd as (
	select
		a.date,
		'WTD' as to_date_type,
		'LY' as time_period,
		'YoY' as comparison_type,
		b.date_ly as xfrm_date
	from yoy_ty_wtd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_lly_wtd as (
	select
		a.date,
		'WTD' as to_date_type,
		'LLY' as time_period,
		'YoY' as comparison_type,
		b.date_lly as xfrm_date
	from yoy_ty_wtd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_ty_ptd as (
	select
		a.date,
		'MTD' as to_date_type,
		'TY' as time_period,
		'YoY' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date >= a.month_start_date
		and b.date <= a.date
),
yoy_ly_ptd as (
	select
		a.date,
		'MTD' as to_date_type,
		'LY' as time_period,
		'YoY' as comparison_type,
		b.date_ly as xfrm_date
	from yoy_ty_ptd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_lly_ptd as (
	select
		a.date,
		'MTD' as to_date_type,
		'LLY' as time_period,
		'YoY' as comparison_type,
		b.date_lly as xfrm_date
	from yoy_ty_ptd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_ty_qtd as (
	select
		a.date,
		'QTD' as to_date_type,
		'TY' as time_period,
		'YoY' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date >= a.quarter_start_date
		and b.date <= a.date
),
yoy_ly_qtd as (
	select
		a.date,
		'QTD' as to_date_type,
		'LY' as time_period,
		'YoY' as comparison_type,
		b.date_ly as xfrm_date
	from yoy_ty_qtd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_lly_qtd as (
	select
		a.date,
		'QTD' as to_date_type,
		'LLY' as time_period,
		'YoY' as comparison_type,
		b.date_lly as xfrm_date
	from yoy_ty_qtd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_ty_ytd as (
	select
		a.date,
		'YTD' as to_date_type,
		'TY' as time_period,
		'YoY' as comparison_type,
		b.date as xfrm_date
	from cte_calendar a
	join cte_calendar b
		on b.date >= a.year_start_date
		and b.date <= a.date
),
yoy_ly_ytd as (
	select
		a.date,
		'YTD' as to_date_type,
		'LY' as time_period,
		'YoY' as comparison_type,
		b.date_ly as xfrm_date
	from yoy_ty_ytd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
yoy_lly_ytd as (
	select
		a.date,
		'YTD' as to_date_type,
		'LLY' as time_period,
		'YoY' as comparison_type,
		b.date_lly as xfrm_date
	from yoy_ty_ytd a
	join cte_calendar b
		on a.xfrm_date = b.date
),
all_xfrm as (
	select * from wow_tw_today
	union all
	select * from wow_lw_today
	union all
	select * from wow_prior_four_weeks_today
	union all
	select * from wow_tw_wtd
	union all
	select * from wow_lw_wtd
	union all
	select * from pop_tp_today
	union all
	select * from pop_lp_today
	union all
	select * from pop_tp_ptd
	union all
	select * from pop_lp_ptd
	union all
	select * from qoq_tq_today
	union all
	select * from qoq_lq_today
	union all
	select * from qoq_tq_qtd
	union all
	select * from qoq_lq_qtd
	union all
	select * from yoy_ty_today
	union all
	select * from yoy_ly_today
	union all
	select * from yoy_lly_today
	union all
	select * from yoy_ty_wtd
	union all
	select * from yoy_ly_wtd
	union all
	select * from yoy_lly_wtd
	union all
	select * from yoy_ty_ptd
	union all
	select * from yoy_ly_ptd
	union all
	select * from yoy_lly_ptd
	union all
	select * from yoy_ty_qtd
	union all
	select * from yoy_ly_qtd
	union all
	select * from yoy_lly_qtd
	union all
	select * from yoy_ty_ytd
	union all
	select * from yoy_ly_ytd
	union all
	select * from yoy_lly_ytd
)
select
    *,
    current_timestamp() as inserted_ts
from all_xfrm
where
	date >= (select min(date) from cte_calendar where year_id >= (select year_id from cte_calendar where date = current_date) - 5)
	and date <= (select max(date) from cte_calendar where year_id <= (select year_id from cte_calendar where date = current_date) + 5)
order by comparison_type, to_date_type, to_date_type, date, xfrm_date