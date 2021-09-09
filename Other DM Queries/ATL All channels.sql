with vars as
(
	select 
	'2020-08-01' as start_date,
	'2020-11-30' as end_date,
	'2020-10-01' as atl_start_date,
	'2020-11-30' as atl_end_date
),

app_org as
(
	select distinct ct.city, abr1, abr2, source, lead_date, lead_phone
	(
		select distinct lower(city) as city
		, lower(trim(source)) as source
		, lead_arrival_time::DATE as lead_date
		, lead_phone
		from growth.stg_all_leads_funnel
		where lead_arrival_time::DATE between (select start_date from vars) and (select end_date from vars)
		and lower(source) in ('app', 'organic')
	) non_dm
	
	left join temp.dm_cities ct on lower(non_dm.city) = lower(ct.city)
), 

dm_leads as
(
	select distinct city, abr1, abr2, source, lead_date, lead_phone
	from 
	(
		select ct.city, ct.abr1, ct.abr2, ch.channel as source, lead_date, unbounce_phone as lead_phone
		from dm.dr_mapped_leads dm 
		left join temp.dm_channels ch on dm.campaign_bucket = ch.campaign_bucket and dm.sem_bau_bucket = ch.sem_bau_bucket
		left join temp.dm_cities ct on lower(dm.city_abr) = lower(ct.abr1)
	)
	where lead_date::DATE between (select start_date from vars) and (select end_date from vars)
	group by city, abr1, abr2, channel, date1, weeknum
	order by city, abr1, abr2, channel, date1, weeknum
),

all_leads as
(
	select * from app_org
	union all
	select * from dm_leads
),

first_lead as
(
	select city, abr1, abr2, source, lead_phone, min(lead_date) as first_lead
	from all_leads
	group by city, abr1, abr2, source, lead_phone
),

select *
from
(
	select a.*, case when lead_date = first_lead then 'New' else 'Repeat' end as lead_type
	from
	(
		select al.*, first_lead
		from all_leads al
		left join first_lead fl on al.lead_phone = fl.lead_phone
	) a
	where lead_date between (select atl_start_date from vars) and (select atl_end_date from vars)
) b;