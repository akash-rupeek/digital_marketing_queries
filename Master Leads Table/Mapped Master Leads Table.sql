truncate table dm.mapped_leads_test;

insert into dm.mapped_leads_test

with dj_data as 
(
	select *
	from
	(
		select lead_phone
		, created_at::date as lead_date
		, loan_type as dj_loan_type
		, case when is_digital_journey = TRUE then 1 else 0 end as is_digital_journey
		, case when google_address is not null then 1 else 0 end as address_completion
		, case when lower(lead_status) = 'completed' then 1 else 0 end as dj_completion
		, json_extract_path_text(custom_params, 'lq_intend', TRUE) nglr_id
		, row_number() over (partition by lead_phone, created_at::date order by updated_at desc) as rnum
		from growth.dm_internal_leads
	)
	where rnum = 1
),

assigned_leads as
(
	select phone_number
	, created_at::date as lead_date
	from dw.chakra_leads_fresh
	where assignedto is not null
	group by 1,2
),

called_leads as
(
	select phone
	, call_time
	, call_time::date as call_date
	from dw.ameyo_call_details
	group by 1,2,3
),

mapped_leads_dj_assign as
(
	select leads.*
	, dj_loan_type, is_digital_journey, address_completion, dj_completion, nglr_id
	, case when assign0.phone_number is not null then 1 else 0 end as same_day_assign
	, case when assign1.phone_number is not null then 1 else 0 end as next_day_assign
	, case when COALESCE(assign0.phone_number, assign1.phone_number) is not null then 1 else 0 end as assigned
	from dm.dm_all_leads_temp_v2 leads
	left join dj_data dj
	on leads.unbounce_phone = dj.lead_phone and leads.lead_date = dj.lead_date

	left join assigned_leads assign0
	on leads.unbounce_phone = assign0.phone_number and leads.lead_date = assign0.lead_date

	left join assigned_leads assign1
	on leads.unbounce_phone = assign1.phone_number and leads.lead_date - assign1.lead_date = 1
),

called_leads_mapped as
(
	select unbounce_phone
	, lead_timestamp
	, min(call_time) as first_call_time
	from dm.dm_all_leads_temp_v2 temp
	inner join called_leads
	on temp.unbounce_phone = called_leads.phone and temp.lead_timestamp <= called_leads.call_time
	group by 1,2
),

all_mapped as
(
	select mapped_leads_dj_assign.*
	, datediff(minute, mapped_leads_dj_assign.lead_timestamp, first_call_time) as TAT
	, case when first_call_time is not null then 1 else 0 end as called_lead
	from mapped_leads_dj_assign
	left join called_leads_mapped
	on mapped_leads_dj_assign.unbounce_phone = called_leads_mapped.unbounce_phone and mapped_leads_dj_assign.lead_timestamp = called_leads_mapped.lead_timestamp
),

utm_mapped_leads AS 
(
    select A.*,
    CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar 
    when lower(utmsource) like '%google - did%' then 'BAU' ELSE B.campaign_name END AS campaign_name,
    CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar 
    when lower(utmsource) like '%google - did%' then 'SEM - BAU'::varchar ELSE B.bucket1 END AS campaign_bucket,
    B.city AS city_abr,
    CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar 
    when lower(utmsource) like '%google - did%' then 'BAU'::varchar  ELSE B.channel_group END AS channel_group,
    B.city_group AS city_group,
    CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.bucket END AS sem_bau_bucket,
    --CASE when lower(utmsource) like '%google - did%' then 'DID'::varchar ELSE B.bucket END AS sem_bau_bucket
    CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar
    when lower(utmsource) like '%google - did%' then 'BAU'::varchar ELSE B.type END AS type,
    CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar 
    when lower(utmsource) like '%google - did%' then 'BAU'::varchar ELSE B.channel_grouping_1 END AS channel_grouping1,
    B.sem_campaigns_buckets AS bau_sub_buckets
    from all_mapped A
    left join 
    (
        SELECT * FROM 
        (select distinct 
        (REPLACE(bucket,',','')) AS bucket,
        (REPLACE(campaign_name,',','')) AS campaign_name,
        (REPLACE(type,',','')) AS type,
        utm_campaign__webflow_,
        (REPLACE(bucket1,',','')) AS bucket1,
        (REPLACE(city,',','')) AS city,
        (REPLACE(city_group,',','')) AS city_group,
        (REPLACE(channel_group,',','')) AS channel_group,
        (REPLACE(channel_grouping_1,',','')) AS channel_grouping_1,
        (REPLACE(sem_campaigns_buckets,',','')) AS sem_campaigns_buckets,
        ROW_NUMBER() OVER(PARTITION BY lower(utm_campaign__webflow_) ORDER BY channel_grouping_1,campaign_name) AS row_num
        from dm.dr_updated_campaign_mapping)
        WHERE row_num = 1 and utm_campaign__webflow_ <> '' and utm_campaign__webflow_ is not null
    ) AS B
    ON lower(A.utmcampaign) = lower(B.utm_campaign__webflow_)
),

channel_mapped_leads as
(
	select *,
	case
		when lower(channel) = 'sem' then bau_sub_buckets
		when lower(channel) = 'facebook' then channel_group
		else channel_level_3 end as channel_level_4

	from
	(
		select *,
		case 
			when lower(channel) = 'sem' then sem_bau_bucket
			when lower(channel) = 'facebook' then channel_source
			when lower(channel) = 'digital affliate' then
				case when channel_level_2 = 'Affiliates-ATL' then utmmedium else utmsource end
			else channel_level_2 end as channel_level_3

		from
		(
			select *,
			case
				when lower(channel) in ('sem', 'youtube', 'discovery', 'gdn') then channel_source
				when lower(channel) = 'website' then
					case when lower(utmsource) like '%blog%' or lower(utmsource) like '%static%' then 'Website-SEO' else 'Website-NonSEO' end
				when lower(channel) = 'google-app' then utmmedium
				when lower(channel) = 'app_organic' then
					case when lower(utmsource) like '%experiment%' then 'App-Experiment' else 'App-Regular' end
				when lower(channel) = 'facebook' then
					case 
						when lower(channel_group) like '%atl%' then 'Facebook-ATL'
						when lower(channel_group) like '%app%' then 'Facebook-App'
						else 'Facebook-Web' end
				when lower(channel) = 'digital affliate' then
					case when lower(utmsource) = 'digitalatl' then 'Affiliates-ATL' else 'Affiliates-DM' end
				else channel end as channel_level_2

			from
			(
				select *,
				case
					when lower(channel_source) like '%discovery%' then 'Discovery'
					when lower(channel_source) like '%yt%' then 'Youtube'
					when lower(channel_source) like '%sem%' then 'SEM'
					when lower(channel_source) like '%gdn%' then 'GDN'
					when lower(channel_source) like '%facebook%' or lower(channel_source) = 'fb - rq' then 'Facebook'
					else channel_source end as channel

				from
				(
					select *
					, case
						when campaign_bucket is null then
						case 
							when lower(trim(utmsource)) in (select distinct affiliate from dm.affiliates_list) then 'Digital Affliate'
							when lower(utmsource) like '%app_paid%' then 'app_paid'
							when lower(utmsource) like '%app_unpaid%' then 'app_organic'
							when lower(utmsource) like '%google%' then 'Google'
							when lower(utmsource) like '%website%' then 'Website'
							when lower(utmsource) like '%webite%' then 'Website'
							when lower(utmsource) like '%customerapp%' then 'Referral'
							when lower(utmsource) like '%sms%' then 'SMS'
							else null end
						else campaign_bucket end as channel_source

					from utm_mapped_leads
				)
			)
		)
	)
)

select *
from channel_mapped_leads
;