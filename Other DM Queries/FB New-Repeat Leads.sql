with vars as
(
	select 
	'2020-11-01' as start_date,
	'2021-02-15' as end_date
),

dm_leads_raw as
(
	select distinct lower(city) as city
	, loan_amount
	, case when lower(otp_verified) in ('true', 'verified') then 1 else 0 end as otp_verified
	, case when scheme_name is not null then 1 else 0 end as scheme_selected
	, "timestamp"
	, timestamp::date as lead_date
	, utmcampaign
	, cta_id
	, case when cta_id >= 2 then 1 else 0 end as qualified_lead
	, unbounce_phone_decrypted
	from growth.dm_leads
	where TIMESTAMP::DATE between (select start_date from vars) and (select end_date from vars)
	and lower(utmsource) like '%facebook%'
),

leads_count as
(
	select *
	, case when first_lead_date = last_lead_date then 'new_lead' else 'repeated_leads' end as lead_type
	from
	(
		select unbounce_phone_decrypted as phone
		, min(timestamp::DATE) as first_lead_date
		, max(timestamp::DATE) as last_lead_date
		, count(distinct(timestamp)) as lead_count
		from dm_leads_raw
		group by unbounce_phone_decrypted
	) l
),

dm_leads as
(
	select x.*, first_lead_date, last_lead_date, lead_count
	from
	(
		select unbounce_phone_decrypted, city, loan_amount, otp_verified, scheme_selected, "timestamp"
		, lead_date, utmcampaign, cta_id, qualified_lead
		from
		(
			select *
			, row_number() over (partition by unbounce_phone_decrypted order by lead_date asc) as first_date
			, row_number() over (partition by unbounce_phone_decrypted, lead_date order by "timestamp" desc) as last_lead_same_day
			from dm_leads_raw
		) xx
		where first_date = 1 and last_lead_same_day = 1
	) x

	left join leads_count
	on x.unbounce_phone_decrypted = leads_count.phone
),


schedules_raw as
(
	select distinct source, transaction_type, scheme
	, coalesce(scheduled_for_date, date, "timestamp") as schedule_date
	, team, loan_amount, doorstep_walkin
	, coalesce(last_updated_time_stamp, "timestamp") as updated_at
	, lower(status) as status
	, mobile_number_decrypted
	from growth.gs_txn_req_all_cities_comb
	where TIMESTAMP::DATE between (select start_date from vars) and (select end_date from vars)
),


schedule_attempts as
(
	select mobile_number_decrypted as schedule_attempts_phone
	, count(distinct(schedule_date)) as schedule_attempts
	from schedules_raw
	group by mobile_number_decrypted
),


schedule_count as
(
	select *
	, case when first_schedule_date = last_schedule_date then 'new_schedule' else 'repeated_schedule' end as schedule_type
	from
	(
		select mobile_number_decrypted as schedule_count_phone
		, min(schedule_date::DATE) as first_schedule_date
		, max(schedule_date::DATE) as last_schedule_date
		, count(distinct(schedule_date)) as schedule_count
		from schedules_raw
		where status = 'scheduled'
		group by mobile_number_decrypted
	) l
),

dm_schedules as
(
	select schraw.*
	, schedule_attempts
	, first_schedule_date, last_schedule_date
	, schedule_count, schedule_type
	from
	(
		select source, transaction_type, scheme
		, schedule_date::DATE as schedule_date, team, loan_amount, doorstep_walkin
		, updated_at, status, mobile_number_decrypted
		from
		(
			select *
			, row_number() over (partition by mobile_number_decrypted order by updated_at asc) as first_rnk
			from schedules_raw
			where status = 'scheduled'
		) y
		where first_rnk = 1
	) schraw

	left join schedule_attempts schattempt on schraw.mobile_number_decrypted = schattempt.schedule_attempts_phone
	left join schedule_count schcount on schraw.mobile_number_decrypted = schcount.schedule_count_phone
),


schedules_mapped as
(
	select mobile_num, max(channel_group) as channel_group
	from dm.dr_mapped_schs
	where "date" between (select start_date from vars) and (select end_date from vars)
	group by mobile_num
),

txns_mapped as
(
	select mobile_num, count(distinct("date")) as txns, max("date") as txn_date
	from dm.dr_mapped_txns
	where "date" between (select start_date from vars) and (select end_date from vars)
	group by mobile_num
)

select dml.*, dms.*, smap.channel_group, tmap.txns, tmap.txn_date
from dm_leads dml
left join dm_schedules dms on dml.unbounce_phone_decrypted = dms.mobile_number_decrypted
left join schedules_mapped smap on dms.mobile_number_decrypted = smap.mobile_num
left join txns_mapped tmap on dms.mobile_number_decrypted = tmap.mobile_num
order by schedule_count desc
;