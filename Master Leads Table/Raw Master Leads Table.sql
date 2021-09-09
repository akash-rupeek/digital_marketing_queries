truncate table dm.dm_all_leads_temp_v2;
insert into   dm.dm_all_leads_temp_v2
with
branch_data as
(
	select distinct user_data_aaid
	, user_data_developer_identity::varchar as user_data_developer_identity
	, install_activity_timestamp_iso
	, last_attributed_touch_data_dollar_3p, last_attributed_touch_data_tilde_ad_set_name
	, last_attributed_touch_data_tilde_ad_name
	, last_attributed_touch_data_tilde_campaign
	, last_attributed_touch_data_tilde_campaign_id
	, last_attributed_touch_data_tilde_channel as channel_sub_bucket
	, user_data_geo_city_en
	, name
	, timestamp_iso
	from dm.campaign_report_branch
	where lower(last_attributed_touch_data_tilde_campaign) in 
	(
		select distinct lower(campaign_name) from dm.facebook_uac_app_campaigns
		--where lower(campaign_name) not like '%retgt%'
	)
),

-- Mapping mobile numbers to different AAIDs got from branch.
-- Mapping campaign and install dates

phone_mapped as
(
	select branch.*, cuser.*
	, core_user_lead_timestamp::date as core_user_date
	-- , datediff(day, install_timestamp, core_user_lead_timestamp) as install_to_lead_diff
	from
	(
		select id, leadid, phone_decrypted, phones_decrypted
		, case when lead_date_rank = 1 then created_at else null end as core_user_lead_timestamp
		from
		(
		select distinct id, leadid, phone_decrypted, phones_decrypted, created_at
		, rank() over (partition by phone_decrypted order by created_at desc) as lead_date_rank
		from dw.core_user
		) cuser_
	) cuser

	inner join

	(
		select aaid, devid, channel_sub_bucket, campaign
		, listagg(city, ' / ') as cities
		, min(install_timestamp) as install_timestamp
		from
		(
			select distinct user_data_aaid as aaid
			, user_data_developer_identity as devid
			, channel_Sub_bucket
			, case
				when lower(last_attributed_touch_data_tilde_campaign) like '%uac%' then last_attributed_touch_data_tilde_campaign_id
				else last_attributed_touch_data_tilde_campaign end as campaign
			, case when lower(user_data_geo_city_en) like '%delhi%' then 'Delhi' else user_data_geo_city_en end as city
			, install_activity_timestamp_iso as install_timestamp
			from branch_data
			where user_data_aaid is not null
		) branch_

		group by 1,2,3,4
	) branch on cuser.id = branch.devid
),

forms_data_paid AS 
(

	select distinct a.sessionid,	a.name,	a.lead_timestamp,	a.gclid,	a.otp_verified_phone,	a.unbounce_phone, a.unbounce_phone_decrypted
	, a.city,	a.scheme_name,	a.loan_amount,	a.google_coordinates,	a.google_address,	a.building_details,	a.street_name,	a.landmark
	, a.loanstartingtime,	a.loanendtime,	a.otp_verified
	, a.utmsource,	b.channel_sub_bucket as utmmedium,	b.campaign as utmcampaign
	, a.applicant_name,	a.weight,	a.quality
	, a.flag_wt_qa,	a.unbounceurl,	a.email,	a.dm_gold_amount,	a.otp_verified_user,	a.pin_code,	a.existing_loan,	a.cta_id,	a.lead_date, a.conc, a.source
	--,case when phone_decrypted is not null then 'APP_Paid'::varchar else 'APP_Unpaid'::varchar end as lead_type 
	from 
	(
		select *
		from 
		(
			select *,row_number() over(partition by unbounce_phone,lead_date order by cta_id asc, lead_timestamp desc) as row1 
			from
			(
				select distinct 
				json_extract_path_text(data, 'id',TRUE) sessionId,
				json_extract_path_text(data, 'applicant_name',TRUE)	    name,
				created_at::timestamp AS lead_timestamp,
				json_extract_path_text(data, 'gclid',TRUE)	    gclid,
				json_extract_path_text(data, 'otp_verified_phone',TRUE)	    otp_verified_phone,
				phone_number::varchar  as	    unbounce_phone,
				phone_number_decrypted::varchar  as	    unbounce_phone_decrypted,
				c.city_name	  as  city,
				json_extract_path_text(data, 'scheme_name',TRUE)	    scheme_name,
				json_extract_path_text(data, 'loan_amount',TRUE)	    loan_amount,
				json_extract_path_text(data, 'google_coordinates',TRUE)	    google_coordinates,
				json_extract_path_text(data, 'google_address',TRUE)	    google_address,
				json_extract_path_text(data, 'building_details',TRUE)	    building_details,
				json_extract_path_text(data, 'street_name',TRUE)	    street_name,
				json_extract_path_text(data, 'landmark',TRUE)	    landmark,
				json_extract_path_text(data, 'loanstartingtime',TRUE)	    loanstartingtime,
				json_extract_path_text(data, 'loanendtime',TRUE)	    loanendtime,
				json_extract_path_text(data, 'otp_verified',TRUE)	    otp_verified,
				case when lower(source) in ('app') then  'APP_PAID'::varchar 
				else 'APP_Paid_Experiment' ::varchar    end as utmSource,
				--case when json_extract_path_text(data, 'utm_source',TRUE) = '' and lower(json_extract_path_text(data, 'unbounce_url',TRUE)) not like '%rupeek web%'  then lead_source else 	json_extract_path_text(data, 'utm_source',TRUE) end 	    utmSource,
				json_extract_path_text(data, 'utm_medium',TRUE)	    utmMedium,
				json_extract_path_text(data, 'utm_campaign',TRUE)	    utmCampaign,
				json_extract_path_text(data, 'applicant_name',TRUE)	    applicant_name,
				json_extract_path_text(data, 'weight',TRUE)	    weight,
				json_extract_path_text(data, 'quality',TRUE)	    quality,
				json_extract_path_text(data, 'flag_wt_qa',TRUE)	    flag_wt_qa,
				json_extract_path_text(data, 'unbounce_url',TRUE)	    unbounceUrl,
				json_extract_path_text(data, 'email',TRUE)	    email,
				json_extract_path_text(data, 'dm_gold_amount',TRUE)	    dm_gold_amount,
				json_extract_path_text(data, 'otp_verified_user',TRUE)	    otp_verified_user,
				json_extract_path_text(data, 'pin_code',TRUE)	    pin_code,
				json_extract_path_text(data, 'existing_loan',TRUE)	    existing_loan,
				case when lower(source) ='app' then 1 ::varchar
				when lower(source) = 'app experiment - otp verified screen' then 2 ::varchar
				when lower(source) = 'app experiment - signup screen' then 3 ::varchar
				else null end as cta_id,
				--json_extract_path_text(data, 'cta_id',TRUE)	    cta_id,
				created_at::date as lead_date,
				concat(phone_number::varchar ,created_at::date) as conc,
				'3chakra'::varchar as source
				from dw.chakra_leads_fresh a 
				left join dw.city c on c.city_id = json_extract_path_text(data, 'city',TRUE)
				where 
				/*(lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%facebook_form%'  or lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%google_form%' or 
				lower(lead_source) like '%sms%' or lower(lead_source) like '%app%' or lower(data) like '%rupeek web%' or*/
				lower(source) like ('%app%')
				and (lower(utm_source) like ('%regular%') or lower(utm_source) is null )
				and created_at::date between '2020-01-01' and getdate()::date-1 and len(phone_number) >= 10
				and json_extract_path_text(data, 'duplicate',TRUE) = ''
			)
		)
		where row1 = 1
	)a

	inner join phone_mapped b
	on a.unbounce_phone_decrypted = b.phone_decrypted 
	and a.lead_date >= b.install_timestamp::date --and app_source = 'app'--and app_lead = 1      -- REMOVED DATE JOIN
),


forms_data AS 
(
	select distinct --created_at,
	json_extract_path_text(data, 'id',TRUE) sessionId,
	json_extract_path_text(data, 'applicant_name',TRUE)	    name,
	created_at::timestamp AS lead_timestamp,
	json_extract_path_text(data, 'gclid',TRUE)	    gclid,
	json_extract_path_text(data, 'otp_verified_phone',TRUE)	    otp_verified_phone,
	phone_number::varchar  as	    unbounce_phone,
	phone_number_decrypted::varchar  as	    unbounce_phone_decrypted,
	json_extract_path_text(json_extract_path_text(data, 'city',TRUE),'value',TRUE)	    city,
	json_extract_path_text(data, 'scheme_name',TRUE)	    scheme_name,
	json_extract_path_text(data, 'loan_amount',TRUE)	    loan_amount,
	json_extract_path_text(data, 'google_coordinates',TRUE)	    google_coordinates,
	json_extract_path_text(data, 'google_address',TRUE)	    google_address,
	json_extract_path_text(data, 'building_details',TRUE)	    building_details,
	json_extract_path_text(data, 'street_name',TRUE)	    street_name,
	json_extract_path_text(data, 'landmark',TRUE)	    landmark,
	json_extract_path_text(data, 'loanstartingtime',TRUE)	    loanstartingtime,
	json_extract_path_text(data, 'loanendtime',TRUE)	    loanendtime,
	json_extract_path_text(data, 'otp_verified',TRUE)	    otp_verified,
	case when lower(json_extract_path_text(data, 'source',TRUE)) like '%website%' and lower(json_extract_path_text(data, 'utm_source',TRUE)) not like '%google%' then lower(json_extract_path_text(data, 'source',TRUE))
	when json_extract_path_text(data, 'utm_source',TRUE) = '' and lower(json_extract_path_text(data, 'unbounce_url',TRUE)) not like '%rupeek web%'  then lead_source 
	else json_extract_path_text(data, 'utm_source',TRUE) end 	    utmSource,
	json_extract_path_text(data, 'utm_medium',TRUE)	    utmMedium,
	json_extract_path_text(data, 'utm_campaign',TRUE)	    utmCampaign,
	json_extract_path_text(data, 'applicant_name',TRUE)	    applicant_name,
	json_extract_path_text(data, 'weight',TRUE)	    weight,
	json_extract_path_text(data, 'quality',TRUE)	    quality,
	json_extract_path_text(data, 'flag_wt_qa',TRUE)	    flag_wt_qa,
	json_extract_path_text(data, 'unbounce_url',TRUE)	    unbounceUrl,
	json_extract_path_text(data, 'email',TRUE)	    email,
	json_extract_path_text(data, 'dm_gold_amount',TRUE)	    dm_gold_amount,
	json_extract_path_text(data, 'otp_verified_user',TRUE)	    otp_verified_user,
	json_extract_path_text(data, 'pin_code',TRUE)	    pin_code,
	json_extract_path_text(data, 'existing_loan',TRUE)	    existing_loan,
	json_extract_path_text(data, 'cta_id',TRUE)	    cta_id,
	created_at::date as lead_date,
	--lead_source,
	concat(phone_number::varchar ,created_at::date) as conc,
	'3chakra'::varchar as source
	from dw.chakra_leads_fresh 
	where 
		(
			lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%facebook_form%'  or lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%google_form%' 
			or lower(lead_source) like '%google%' or lower(lead_source) like '%facebook%' or lower(data) like '%rupeek web%'
			or lower(lead_source) in ('digital-journey')
			or lower(lead_source) in (select distinct affiliate from dm.affiliates_list)
			or lower(json_extract_path_text(data, 'source',TRUE)) like '%website%'
		)
	and created_at::date between '2020-01-01' and getdate()::date-1 and len(phone_number) >= 10
)
,


/*forms_data_paid_experiment AS 
(

select distinct a.sessionid,	a.name,	a.lead_timestamp,	a.gclid,	a.otp_verified_phone,	a.unbounce_phone, a.unbounce_phone_decrypted
	, a.city,	a.scheme_name,	a.loan_amount,	a.google_coordinates,	a.google_address,	a.building_details,	a.street_name,	a.landmark
	, a.loanstartingtime,	a.loanendtime,	a.otp_verified,	a.utmsource,	a.utmmedium,	a.utmcampaign,	a.applicant_name,	a.weight,	a.quality
	, a.flag_wt_qa,	a.unbounceurl,	a.email,	a.dm_gold_amount,	a.otp_verified_user,	a.pin_code,	a.existing_loan,	a.cta_id,	a.lead_date, a.conc, a.source
	--,case when phone_decrypted is not null then 'APP_Paid'::varchar else 'APP_Unpaid'::varchar end as lead_type 
from (
 select * from (
 select *,row_number() over(partition by unbounce_phone,lead_date order by cta_id asc,lead_date asc) as row1 
 from(
	select distinct 
	json_extract_path_text(data, 'id',TRUE) sessionId,
	json_extract_path_text(data, 'applicant_name',TRUE)	    name,
	created_at::timestamp AS lead_timestamp,
	json_extract_path_text(data, 'gclid',TRUE)	    gclid,
	json_extract_path_text(data, 'otp_verified_phone',TRUE)	    otp_verified_phone,
	phone_number::varchar  as	    unbounce_phone,
	phone_number_decrypted::varchar  as	    unbounce_phone_decrypted,
	c.city_name	  as  city,
	json_extract_path_text(data, 'scheme_name',TRUE)	    scheme_name,
	json_extract_path_text(data, 'loan_amount',TRUE)	    loan_amount,
	json_extract_path_text(data, 'google_coordinates',TRUE)	    google_coordinates,
	json_extract_path_text(data, 'google_address',TRUE)	    google_address,
	json_extract_path_text(data, 'building_details',TRUE)	    building_details,
	json_extract_path_text(data, 'street_name',TRUE)	    street_name,
	json_extract_path_text(data, 'landmark',TRUE)	    landmark,
	json_extract_path_text(data, 'loanstartingtime',TRUE)	    loanstartingtime,
	json_extract_path_text(data, 'loanendtime',TRUE)	    loanendtime,
	json_extract_path_text(data, 'otp_verified',TRUE)	    otp_verified,
	case when lower(source) in ('app') then  'APP_PAID'::varchar 
	else 'APP_Paid_Experiment' ::varchar    end as utmSource,
	--case when json_extract_path_text(data, 'utm_source',TRUE) = '' and lower(json_extract_path_text(data, 'unbounce_url',TRUE)) not like '%rupeek web%'  then lead_source else 	json_extract_path_text(data, 'utm_source',TRUE) end 	    utmSource,
	json_extract_path_text(data, 'utm_medium',TRUE)	    utmMedium,
	json_extract_path_text(data, 'utm_campaign',TRUE)	    utmCampaign,
	json_extract_path_text(data, 'applicant_name',TRUE)	    applicant_name,
	json_extract_path_text(data, 'weight',TRUE)	    weight,
	json_extract_path_text(data, 'quality',TRUE)	    quality,
	json_extract_path_text(data, 'flag_wt_qa',TRUE)	    flag_wt_qa,
	json_extract_path_text(data, 'unbounce_url',TRUE)	    unbounceUrl,
	json_extract_path_text(data, 'email',TRUE)	    email,
	json_extract_path_text(data, 'dm_gold_amount',TRUE)	    dm_gold_amount,
	json_extract_path_text(data, 'otp_verified_user',TRUE)	    otp_verified_user,
	json_extract_path_text(data, 'pin_code',TRUE)	    pin_code,
	json_extract_path_text(data, 'existing_loan',TRUE)	    existing_loan,
	case when lower(source) ='app' then 1 ::varchar
    when lower(source) = 'app experiment - otp verified screen' then 2 ::varchar
    when lower(source) = 'app experiment - signup screen' then 3 ::varchar
    else null end as cta_id,
	--json_extract_path_text(data, 'cta_id',TRUE)	    cta_id,
	created_at::date as lead_date,
	concat(phone_number::varchar ,created_at::date) as conc,
	'3chakra'::varchar as source
	from dw.chakra_leads_fresh a 
	left join dw.city c on c.city_id = json_extract_path_text(data, 'city',TRUE)
	where (lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%facebook_form%'  or lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%google_form%' or 
		lower(lead_source) like '%sms%' or lower(lead_source) like '%app%' or lower(data) like '%rupeek web%' or
		 lower(source) like ('%app%')
		 and (lower(utm_source) like ('%regular%') or lower(utm_source) is null )
	and created_at::date between '2020-01-01' and getdate()::date-1 and len(phone_number) >= 10
	and json_extract_path_text(data, 'duplicate',TRUE) = ''
	))
where row1 = 1
)a
inner join leads_mapped on a.unbounce_phone_decrypted = phone_decrypted and a.lead_date = app_lead_date --and app_lead = 1
),*/

forms_data_unpaid AS 
(

select distinct a.sessionid,	a.name,	a.lead_timestamp,	a.gclid,	a.otp_verified_phone,	a.unbounce_phone, a.unbounce_phone_decrypted
	, a.city,	a.scheme_name,	a.loan_amount,	a.google_coordinates,	a.google_address,	a.building_details,	a.street_name,	a.landmark
	, a.loanstartingtime,	a.loanendtime,	a.otp_verified,	a.utmsource,	a.utmmedium,	a.utmcampaign,	a.applicant_name,	a.weight,	a.quality
	, a.flag_wt_qa,	a.unbounceurl,	a.email,	a.dm_gold_amount,	a.otp_verified_user,	a.pin_code,	a.existing_loan,	a.cta_id,	a.lead_date, a.conc, a.source
	
from (
 select * from (
 select *,row_number() over(partition by unbounce_phone,lead_date order by cta_id asc,lead_date asc) as row1 
 from(
	select distinct 
	json_extract_path_text(data, 'id',TRUE) sessionId,
	json_extract_path_text(data, 'applicant_name',TRUE)	    name,
	created_at::timestamp AS lead_timestamp,
	json_extract_path_text(data, 'gclid',TRUE)	    gclid,
	json_extract_path_text(data, 'otp_verified_phone',TRUE)	    otp_verified_phone,
	phone_number::varchar  as	    unbounce_phone,
	phone_number_decrypted::varchar  as	    unbounce_phone_decrypted,
	c.city_name	  as  city,
	json_extract_path_text(data, 'scheme_name',TRUE)	    scheme_name,
	json_extract_path_text(data, 'loan_amount',TRUE)	    loan_amount,
	json_extract_path_text(data, 'google_coordinates',TRUE)	    google_coordinates,
	json_extract_path_text(data, 'google_address',TRUE)	    google_address,
	json_extract_path_text(data, 'building_details',TRUE)	    building_details,
	json_extract_path_text(data, 'street_name',TRUE)	    street_name,
	json_extract_path_text(data, 'landmark',TRUE)	    landmark,
	json_extract_path_text(data, 'loanstartingtime',TRUE)	    loanstartingtime,
	json_extract_path_text(data, 'loanendtime',TRUE)	    loanendtime,
	json_extract_path_text(data, 'otp_verified',TRUE)	    otp_verified,
	case when lower(source) in ('app') then  'APP_UNPAID'::varchar 
	else 'APP_Unpaid_Experiment' ::varchar end as    utmSource,
	--case when json_extract_path_text(data, 'utm_source',TRUE) = '' and lower(json_extract_path_text(data, 'unbounce_url',TRUE)) not like '%rupeek web%'  then lead_source else 	json_extract_path_text(data, 'utm_source',TRUE) end 	    utmSource,
	json_extract_path_text(data, 'utm_medium',TRUE)	    utmMedium,
	json_extract_path_text(data, 'utm_campaign',TRUE)	    utmCampaign,
	json_extract_path_text(data, 'applicant_name',TRUE)	    applicant_name,
	json_extract_path_text(data, 'weight',TRUE)	    weight,
	json_extract_path_text(data, 'quality',TRUE)	    quality,
	json_extract_path_text(data, 'flag_wt_qa',TRUE)	    flag_wt_qa,
	json_extract_path_text(data, 'unbounce_url',TRUE)	    unbounceUrl,
	json_extract_path_text(data, 'email',TRUE)	    email,
	json_extract_path_text(data, 'dm_gold_amount',TRUE)	    dm_gold_amount,
	json_extract_path_text(data, 'otp_verified_user',TRUE)	    otp_verified_user,
	json_extract_path_text(data, 'pin_code',TRUE)	    pin_code,
	json_extract_path_text(data, 'existing_loan',TRUE)	    existing_loan,
	case when lower(source) ='app' then 1 ::varchar
    when lower(source) = 'app experiment - otp verified screen' then 2 ::varchar
    when lower(source) = 'app experiment - signup screen' then 3 ::varchar
    else null end as cta_id,
	--json_extract_path_text(data, 'cta_id',TRUE)	    cta_id,
	created_at::date as lead_date,
	concat(phone_number::varchar ,created_at::date) as conc,
	'3chakra'::varchar as source
	from dw.chakra_leads_fresh a 
	left join dw.city c on c.city_id = json_extract_path_text(data, 'city',TRUE)
	where /*(lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%facebook_form%'  or lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%google_form%' or 
		lower(lead_source) like '%sms%' or lower(lead_source) like '%app%' or lower(data) like '%rupeek web%' or*/
		 lower(source) like ('%app%')
		 and (lower(utm_source) like ('%regular%') or lower(utm_source) is null )
	and created_at::date between '2020-01-01' and getdate()::date-1 and len(phone_number) >= 10
	and json_extract_path_text(data, 'duplicate',TRUE) = ''
	))
where row1 = 1
)a
left join phone_mapped b 
	on a.unbounce_phone_decrypted = b.phone_decrypted 
	and a.lead_date >= b.install_timestamp::date 
where b.phone_decrypted is null
)

,


dm_leads AS
(
	select DISTINCT sessionId,
	name,
	timestamp::timestamp AS lead_timestamp,
	gclid,
	otp_verified_phone,
	unbounce_phone::varchar unbounce_phone,
	unbounce_phone_decrypted::varchar  as unbounce_phone_decrypted,
	city,
	scheme_name,
	loan_amount,
	google_coordinates,
	google_address,
	building_details,
	street_name,
	landmark,
	loanstartingtime,
	loanendtime,
	otp_verified,
	CASE when lower(unbounceurl) like '%rupeek web%' then 'website'::varchar ELSE utmSource::VARCHAR END AS utmSource,
	utmMedium,
	utmCampaign,
	name as applicant_name,
	weight,
	quality,
	flag_wt_qa,
	unbounceUrl,
	email,
	dm_gold_amount,
	otp_verified_user,
	pin_code,
	existing_loan,
	cta_id,
	timestamp :: date AS lead_date,
	concat(unbounce_phone::varchar,timestamp::date) as conc,
	'2dmleads'::varchar as source
	FROM growth.dm_leads
	WHERE
	timestamp :: date BETWEEN '2020-01-01'
	AND getdate() -1
	AND 
	(
		lower(utmSource) like '%goog%'
		or lower(utmSource) like '%facebook%'
		or lower(utmSource) in ('google', 'facebook', 'fb')
		or lower(utmSource) in (select distinct affiliate from dm.affiliates_list)
		or lower(unbounceurl) like '%rupeek web%'
		--or lower(source) like ('%app%')
	)
	AND "timestamp"::date >= '2020-01-01'
	AND len(unbounce_phone) >= 10

),

webflow_ga AS
(
	select distinct * 
	from 
	(
		SELECT DISTINCT
		sessionId,
		name,
		timestamp::timestamp AS lead_timestamp,
		gclid,
		otp_verified_phone,
		unbounce_phone::varchar AS unbounce_phone,
		unbounce_phone_decrypted::varchar  AS unbounce_phone_decrypted,
		city,
		scheme_name,
		loan_amount,
		google_coordinates,
		google_address,
		building_details,
		street_name,
		landmark,
		loanstartingtime,
		loanendtime,
		otp_verified,
		CASE when lower(unbounceurl) like '%rupeek web%' then 'website'::varchar ELSE utmSource::VARCHAR END AS utmSource,
		utmMedium,
		utmCampaign,
		applicant_name,
		weight,
		quality,
		flag_wt_qa,
		unbounceUrl,
		email,
		dm_gold_amount,
		otp_verified_user,
		pin_code,
		existing_loan,
		cta_id,
		date AS lead_date,
		concat(unbounce_phone::varchar,coalesce(timestamp::date,date)) as conc,
		concat(1,source::varchar) AS source
		from
		dm.dr_webflow_ga
		WHERE
		timestamp :: date BETWEEN '2020-01-01'
		AND getdate() -1
		AND 
		(
			lower(utmSource) like '%goog%'
			or lower(utmSource) like '%facebook%'
			or lower(utmSource) in ('google', 'facebook', 'fb')
			or lower(utmSource) in (select distinct affiliate from dm.affiliates_list)
			or lower(unbounceurl) like '%rupeek web%'
			or lower(utmsource) in ('sms', 'smsrq','-')
		)
		AND coalesce("timestamp"::date,date) >= '2020-01-01'
		AND len(unbounce_phone) >= 10
	)

	UNION 

	select *
	from forms_data
),

all_leads_funnel_dm AS 
(
	SELECT DISTINCT
	'' AS sessionId,
	'' AS name,
	lead_arrival_time::timestamp AS lead_timestamp,
	'' AS gclid,
	'' AS otp_verified_phone,
	lead_phone::varchar  AS  unbounce_phone,
	right(primaryphonenumber_decrypted,10)::varchar AS  unbounce_phone_decrypted,
	lower(city_growth)::varchar AS city,
	'' AS scheme_name,
	'' AS loan_amount,
	'' AS google_coordinates,
	'' AS google_address,
	'' AS building_details,
	'' AS street_name,
	'' AS landmark,
	'' AS loanstartingtime,
	'' AS loanendtime,
	'' AS otp_verified,
	source::varchar AS utmSource,
	'' AS utmMedium,
	campaign::varchar AS utmCampaign,
	'' AS applicant_name,
	'' AS weight,
	'' AS quality,
	'' AS flag_wt_qa,
	'' AS unbounceUrl,
	'' AS email,
	'' AS dm_gold_amount,
	'' AS otp_verified_user,
	'' AS pin_code,
	'' AS existing_loan,
	null AS cta_id,
	lead_arrival_time::date as lead_date,
	concat(lead_phone::varchar ,lead_arrival_time::date) as conc,
	'4all_leads'::varchar AS source
	FROM growth.stg_all_leads_funnel
	WHERE 
	(
		lower(source) like '%goog%' 
		OR lower(source) in ('google' )
		OR lower(source) in (select distinct affiliate from dm.affiliates_list)
		OR lower(source) like '%fb%' 
		OR lower(source) LIKE '%facebook%' 
		OR lower(source) like '%web%' 
		OR lower(source) like '%you%' 
		OR lower(source) like '%generic%' 
		OR lower(source) like '%brand%' 
		OR lower(source) like '%website%' 
		or lower(source) in ('sms', 'smsrq','sms_ol','sms_ob','sms_exp',
		/*,'sms_misc','app','app experiment - signup screen','app experiment - otp verified screen','cibil',*/'sms','sms_ol','sms_ob','sms_misc','sms_exp')
	)
	AND lead_arrival_time::date >= '2020-01-01'
	AND len(primaryphonenumber_decrypted) >= 10
	AND data_source <>'dm_missing_leads_db_dr'
)
--select max(lead_timestamp::date) from dm_leads;
,
	
/*asr_missing_leads AS 
(
	SELECT DISTINCT
	'' AS sessionId,
	'' AS name,
	"timestamp"::TIMESTAMP AS lead_timestamp,
	'' AS gclid,
	'' AS otp_verified_phone,
	mobile  AS  unbounce_phone,
	right(mobile,10) AS  unbounce_phone_decrypted,
	lower(city) AS city,
	'' AS scheme_name,
	'' AS loan_amount,
	'' AS google_coordinates,
	'' AS google_address,
	'' AS building_details,
	'' AS street_name,
	'' AS landmark,
	'' AS loanstartingtime,
	'' AS loanendtime,
	'' AS otp_verified,
	source AS utmSource,
	'' AS utmMedium,
	'' AS utmCampaign,
	'' AS applicant_name,
	'' AS weight,
	'' AS quality,
	'' AS flag_wt_qa,
	'' AS unbounceUrl,
	'' AS email,
	'' AS dm_gold_amount,
	'' AS otp_verified_user,
	'' AS pin_code,
	'' AS existing_loan,
	null AS cta_id,
	timestamp::date as lead_date,
	concat(mobile::varchar ,timestamp::date) as conc,
	source
	-- ,'Unpaid'::varchar as lead_type
	FROM dm.asr_missing_leads
	WHERE   TIMESTAMP::date >= '2019-01-01'
	AND len(mobile) >= 10
),*/

Referral_leads_dm AS 
(
	SELECT DISTINCT
	a.id AS sessionId,
	referralcode AS name,
	a.created_at::timestamp AS lead_timestamp,
	'' AS gclid,
	'' AS otp_verified_phone,
	phone::varchar AS  unbounce_phone,
	right(phone_decrypted,10) AS  unbounce_phone_decrypted,
	case when city_name is null then '' else city_name end AS city,
	schemeid AS scheme_name,
	'' AS loan_amount,
	'' AS google_coordinates,
	'' AS google_address,
	'' AS building_details,
	'' AS street_name,
	'' AS landmark,
	'' AS loanstartingtime,
	'' AS loanendtime,
	'' AS otp_verified,
	b.source::varchar AS utmSource,
	'' AS utmMedium,
	role::varchar AS utmCampaign,
	'' AS applicant_name,
	'' AS weight,
	'' AS quality,
	'' AS flag_wt_qa,
	'' AS unbounceUrl,
	'' AS email,
	'' AS dm_gold_amount,
	'' AS otp_verified_user,
	'' AS pin_code,
	'' AS existing_loan,
	null AS cta_id,
	a.created_at::date as lead_date,
	concat(phone::varchar ,a.created_at::date) as conc,
	'Ref_leads'::varchar AS source
	--,'PAID'::varchar as lead_type
	from dw.core_referral a
	left join dw.core_user b on a.user_id = b.id
	left join dw.city c on c.city_id = b.cityid
	where lower(role) = 'customer'
	and a.created_at::date >= '2019-01-01'
	and referralcode is not null
	AND len(phone) >= 10
	AND b.source <>'dm_missing_leads_db_dr'
),
	
/*
App_leads_dm AS 
(
	SELECT DISTINCT
	a.id ::varchar AS sessionId,
	'' AS name,
	a.created_at::timestamp AS lead_timestamp,
	'' AS gclid,
	'' AS otp_verified_phone,
	phone::varchar AS  unbounce_phone,
	phone_decrypted::VARCHAR AS  unbounce_phone_decrypted,
	case when city_name is null then '' else city_name end AS city,
	schemeid::VARCHAR AS scheme_name,
	'' AS loan_amount,
	'' AS google_coordinates,
	'' AS google_address,
	'' AS building_details,
	'' AS street_name,
	'' AS landmark,
	'' AS loanstartingtime,
	'' AS loanendtime,
	'' AS otp_verified,
	'App'::varchar AS utmSource,
	'' AS utmMedium,
	role::varchar AS utmCampaign,
	'' AS applicant_name,
	'' AS weight,
	'' AS quality,
	'' AS flag_wt_qa,
	'' AS unbounceUrl,
	'' AS email,
	'' AS dm_gold_amount,
	'' AS otp_verified_user,
	'' AS pin_code,
	'' AS existing_loan,
	null AS cta_id,
	a.created_at::date as lead_date,
	concat(phone::varchar ,a.created_at::date) as conc,
	'App_Leads'::varchar AS source
	, 1::int as priority
	--,'Unpaid'::varchar as lead_type
	from dw.core_customerloanrequest a
	left join dw.core_user b on a.requested_id = b.id
	left join dw.city c on c.city_id = b.cityid
	where lower(role) = 'customer'
	--and lower(city_name) not like ('%other%')
	and isrpkquick not in (TRUE)
	and a.created_at::date >= '2019-01-01'
	AND len(phone) >= 10
	--AND b.source <>'dm_missing_leads_db_dr'
),

SMS_leads_dm AS 
(
	SELECT DISTINCT
	a.id ::varchar AS sessionId,
	'' AS name,
	a.call_time::timestamp AS lead_timestamp,
	'' AS gclid,
	'' AS otp_verified_phone,
	phone::varchar AS  unbounce_phone,
	phone_decrypted::VARCHAR AS  unbounce_phone_decrypted,
	case when city is null then '' else city end AS city,
	''::VARCHAR AS scheme_name,
	'' AS loan_amount,
	'' AS google_coordinates,
	'' AS google_address,
	'' AS building_details,
	'' AS street_name,
	'' AS landmark,
	'' AS loanstartingtime,
	'' AS loanendtime,
	'' AS otp_verified,
	'sms'::varchar AS utmSource,
	'' AS utmMedium,
	''::varchar AS utmCampaign,
	'' AS applicant_name,
	'' AS weight,
	'' AS quality,
	'' AS flag_wt_qa,
	'' AS unbounceUrl,
	'' AS email,
	'' AS dm_gold_amount,
	'' AS otp_verified_user,
	'' AS pin_code,
	'' AS existing_loan,
	null AS cta_id,
	a.call_time::date as lead_date,
	concat(phone::varchar ,a.call_time::date) as conc,
	'Sms_Leads'::varchar AS source
	, 1::int as priority
	from dw.ameyo_acd_call_details a
	left join growth.inbound_did_number on did_number = dnis
	where right(a.dnis,10) in (
	'8068717446','8068717427','8068717417','8068717406','8068717456','8068717421','8068717451','8068717441','8068717432'
	,'8068717419','8068717413','8068717449','8068717434','8068372881','8068717453','8068717525','8068717458','8068717407'
	,'8068717418','8068717442','8068187515','8068187522','8068187544','8068178859','8068178899','8068187560','8068178885'
	,'8068187598','8068187579','8068178807','8068187574','8068187517','8068186814','8068186826','8068178808','8068717424'
	,'8068717423','8068178803','8068717585','8068717416','8068717584','8068717580','8068717586','8068717573','8068717583'
	,'8068717574','8068717431','8068717576','8068717582','8068717575','8068717426','8068187534','8068187535','8068187536'
	,'8068187537','8068187538','8068187577','8068187580','8068187585')

	and a.call_time::date >= '2019-01-01'
	AND len(a.phone) >= 10
	--AND b.source <>'dm_missing_leads_db_dr'
),
*/




non_ga AS 
(
	select  *,2::int as priority from webflow_ga 
	where lower(source) in ('1webflow', '3chakra')
	union 
	select *,2::int as priority from forms_data_paid
	UNION
	select  *,3::int as priority from forms_data_unpaid 
	union 
	select *,1::int as priority from Referral_leads_dm
	union 
	select  *,2::int as priority from dm_leads 
	union
	select *,2::int as priority from all_leads_funnel_dm
	/*where conc not in 
		(
			select distinct conc from webflow_ga where lower(source)  in ('1webflow', '3chakra')
			union 
			select distinct conc from dm_leads 
		)*/
)


,

final as 
( 
	select sessionId,
	name,
	lead_timestamp,
	gclid,
	otp_verified_phone,
	unbounce_phone,
	unbounce_phone_decrypted,
	city,
	scheme_name,
	loan_amount,
	google_coordinates,
	google_address,
	building_details,
	street_name,
	landmark,
	loanstartingtime,
	loanendtime,
	otp_verified,
	case when lower(utmSource) = 'digital-journey' then 'google' else utmSource end as utmSource,
	utmMedium,
	case when lower(utmSource) = 'digital-journey' then 'digital-journey' else utmCampaign end as utmCampaign,
	applicant_name,
	weight,
	quality,
	flag_wt_qa,
	unbounceUrl,
	email,
	dm_gold_amount,
	otp_verified_user,
	pin_code,
	existing_loan,
	cta_id,
	lead_date,
	conc,
	source,
	max_cta_id
	,row_numb2
	,priority
	,row_numb3
	from 
	(
		select distinct *
		, row_number() over(partition by unbounce_phone,lead_date,lower(channel)) as row_numb2
		, case when lower(channel) like ('%app%') then row_number () over ( partition by unbounce_phone,lead_date order by priority asc) else 1 end as row_numb3, 
		max((case when cta_id is null or cta_id in ('', '-') THEN -1 ELSE cta_id::bigint END)::float) over(partition by unbounce_phone, lead_date, utmsource) AS max_cta_id
		from
			(
			SELECT *
			,case when lower(utmsource) like '%website%' then 'website' 
			   when priority = 1 then 'referral'
			   when lower(utmsource) like ('%sms%') then 'sms'
			   when lower (utmsource) like ('%google%') then 'google'
			   else lower(utmsource) end as Channel
			from non_ga
			/*union 
			select * from webflow_ga 
			where lower(source) = '1ga' 
			and conc not in (select distinct conc from non_ga)*/
		)
	)
	where row_numb2 = 1 and row_numb3 = 1 and 
	lead_date < getdate()::date
)

select *--unbounce_phone,lead_date,utmsource,row_numb2,row_numb3,priority
from FINAL
--where lead_date between '2021-07-01' and '2021-07-13'
--order by unbounce_phone,lead_date,utmsource
;