with funnel as
(
	select
	distinct
	unbounce_phone_decrypted,
	lead_date,
	city_abr,
	utmsource,
	utmcampaign,
	datepart(year,lead_date)::int*100+datepart(month,lead_date)::int as month,
	datepart(year,lead_date)::int*100+weeknum::int as week,
	max_cta_id,
	sch_type,
	sch,
	sch_date,
	txn_type,
	txn,
	txn_date,
	max_disposition,
	case when coalesce(first_call, last_call_date) is not null then 1 else 0 end as called,
	case when otp_verified_user = 'TRUE' then 1 else 0 end as otp_verified,
	case when city_abr is null or lower(city_abr) in ('others', 'other', 'oth', '0', 0) then 0 else 1 end as servicable_city,
	case when (lower(max_disposition) is null or lower(max_disposition) in
	('customer disconnected', 
	'switchoff', 
	'could not complete call', 
	'cdc', 
	'test call', 
	'wrong no', 
	'no gold loan requirement', 
	'rnr', 
	'out of cluster', 
	'invalid no', 
	'support related', 
	'not reachable', 
	'ogl', 
	'pawn shop', 
	'busy', 
	'call dropped',
	'null')) then 0 else 1 end as servicable_disposition
	from dm.dr_affiliates_leads_disposition_v2
)

select utmsource, utmcampaign, lead_date, month, week, city_abr
, count(distinct(unbounce_phone_decrypted)) as leads
, count(distinct(case when max_cta_id = 2 then unbounce_phone_decrypted else null end)) as cta_id_2_leads
, count(distinct(case when max_cta_id = 2 and servicable_city = 1 then unbounce_phone_decrypted else null end)) as servicable_leads
, count(distinct(case when max_cta_id = 2 and servicable_city = 1 and servicable_disposition = 1 then unbounce_phone_decrypted else null end)) as qualified_lead
, count(distinct(case when called = 1 then unbounce_phone_decrypted else null end)) as called_leads
, sum(sch) as total_schedules
, sum(case when lower(sch_type) = 'fresh' then sch else 0 end) as fresh_schedule
, sum(case when lower(sch_type) = 'takeover' then sch else 0 end) as takeover_schedule
, sum(txn) as total_txn
, sum(case when lower(txn_type) = 'fresh' then txn else 0 end) as fresh_txn
, sum(case when lower(txn_type) = 'takeover' then txn else 0 end) as takeover_txn
from funnel
group by 1,2,3,4,5,6
;