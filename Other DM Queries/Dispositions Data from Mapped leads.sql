with vars as
(
	select '2021-08-01' as start_date
	, '2021-08-23' as end_date
	, '%discovery%' as channel
)


select unbounce_phone_decrypted
, campaign_name
, leads.lead_date
, city_abr
, case when max_disposition is null then 'No Dispositions' else max_disposition end as disposition
, case when max_status is null then 'No Status' else max_status end as status
from

(
	select unbounce_phone
	, unbounce_phone_decrypted
	, campaign_name
	, lead_date
	, city_abr
	from dm.dr_mapped_leads
	where lead_date between (select start_date from vars) and (select end_date from vars)
	and lower(channel_group) like (select channel from vars)
) leads

left join

(
	select distinct *
	from
	(
		select modified_customer_phone
		, lead_arrival_time::date as lead_date
		, lower(lead_city) as city
		, max_disposition
		, max_status
		, row_number() over (partition by modified_customer_phone order by call_time desc) as rn
		from growth.rpt_daily_dispositions_dca
		where lead_arrival_time::date between (select start_date from vars) and (select end_date from vars)
		and max_disposition is not null
	) x
	where rn = 1
) dispos

on leads.unbounce_phone = dispos.modified_customer_phone and leads.lead_date <= dispos.lead_date
;