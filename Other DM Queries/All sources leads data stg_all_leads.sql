with vars as
(
	select 
	'2020-11-01' as start_date,
	'2020-11-30' as end_date
),

all_leads as
(
	select lower(city) as city
	, transaction_type
	, customer_type
	, source
	, lead_arrival_time::DATE as lead_date
	, extract(week from lead_arrival_time::timestamp) as lead_week
	, lead_phone
	, scheduled_date::DATE as scheduled_date
	, extract(week from scheduled_date) as scheduled_week
	, cashtransferred_date::DATE as txn_date
	, extract(week from cashtransferred_date) as txn_week
	, phone_growth as sch_phone
	, phone as txn_phone
	from growth.stg_all_leads_funnel
	where lead_arrival_time::DATE between (select start_date from vars) and (select end_date from vars)
	and lower(source) in ('app', 'organic')
), 

leads as
(
	select distinct city
	, source
	, lead_date
	, lead_week
	, lead_phone
	from all_leads
	where lead_date is not null
),

schedules as
(
	select distinct city
	, source
	, scheduled_date
	, scheduled_week
	, sch_phone
	from all_leads
	where scheduled_date is not null
),

transactions as
(
	select distinct city
	, source
	, customer_type
	, txn_date
	, txn_week
	, txn_phone
	from all_leads
	where txn_date is not null
)

select * from
(
	select ct.city, abr1, abr2
	, source, lead_date, lead_week
	, leads
	, schedule_bd, schedule_ce, schedule_same_day, schedule_same_week
	, txn_bd, txn_ce, txn_same_day, txn_same_week
	, new_txn_bd, repeat_txn_bd
	from
	(
		select leads.city, leads.source, lead_date, lead_week
		, count(distinct(lead_phone)) as leads
		, count(distinct(case when lead_date = scheduled_date then sch_phone else null end)) as schedule_bd
		, count(distinct(case when lead_phone = sch_phone then sch_phone else null end)) as schedule_ce
		, count(distinct(case when lead_date = scheduled_date and lead_phone = sch_phone then sch_phone else null end)) as schedule_same_day
		, count(distinct(case when lead_week = scheduled_week and lead_phone = sch_phone then sch_phone else null end)) as schedule_same_week
		, count(distinct(case when lead_date = txn_date then txn_phone else null end)) as txn_bd
		, count(distinct(case when lead_phone = txn_phone then txn_phone else null end)) as txn_ce
		, count(distinct(case when lead_date = txn_date and lead_phone = txn_phone then txn_phone else null end)) as txn_same_day
		, count(distinct(case when lead_week = txn_week and lead_phone = txn_phone then txn_phone else null end)) as txn_same_week
		, count(distinct(case when lead_date = txn_date and lower(customer_type) = 'new' then txn_phone else null end)) as new_txn_bd
		, count(distinct(case when lead_date = txn_date and lower(customer_type) = 'repeat' then txn_phone else null end)) as repeat_txn_bd
		from leads
		left join schedules on leads.city = schedules.city and leads.source = schedules.source
		left join transactions on leads.city = transactions.city and leads.source = transactions.source
		group by leads.city, leads.source, lead_date, lead_week, customer_type
		order by leads.city, leads.source, lead_date, lead_week, customer_type
	) non_dm
	left join temp.dm_cities ct on lower(non_dm.city) = lower(ct.city)
) z

;