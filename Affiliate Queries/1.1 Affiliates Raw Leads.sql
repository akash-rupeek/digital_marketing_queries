TRUNCATE TABLE  dm.dr_affiliates_leads_raw_leads;

INSERT INTO  dm.dr_affiliates_leads_raw_leads

with affiliates as
(
	select 'intellactads' as affiliate
	union
	select 'affipedia' as aff2
	union
	select 'yoads' as aff3
	union
	select 'timesinternet' as aff4
	union
	select 'valueleaf' as aff5
	union
	select 'sensedigital' as aff6
	union
	select 'icubeswire' as aff7
),

forms_data AS 
(
select distinct 
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
case when json_extract_path_text(data, 'utm_source',TRUE) = '' and lower(json_extract_path_text(data, 'unbounce_url',TRUE)) not like '%rupeek web%'  then lead_source else 	json_extract_path_text(data, 'utm_source',TRUE) end 	    utmSource,
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
concat(phone_number::varchar ,created_at::date) as conc,
'3chakra'::varchar as source
from dw.chakra_leads_fresh 
where (lower(lead_source) in (select distinct lower(affiliate) from affiliates)
	)
and created_at::date between '2021-01-01' and getdate()::date-1 and len(phone_number) >= 10
),

dm_leads AS
(select DISTINCT sessionId,
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
	    timestamp :: date BETWEEN '2019-01-01'
	    AND getdate() -1
	    AND lower(utmSource) in (select distinct lower(affiliate) from affiliates)
	    AND "timestamp"::date >= '2019-01-01'
		AND len(unbounce_phone) >= 10

),


webflow_ga AS
(select distinct * from 
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
	    AND lower(utmSource) in (select distinct lower(affiliate) from affiliates)
	    AND coalesce("timestamp"::date,date) >= '2020-01-01'
		AND len(unbounce_phone) >= 10

)
UNION 

(select * from forms_data )
),


all_leads_funnel_dm AS (
SELECT DISTINCT
	    '' AS sessionId,
	    '' AS name,
	    lead_arrival_time::timestamp AS lead_timestamp,
	    '' AS gclid,
	    '' AS otp_verified_phone,
	    lead_phone::varchar  AS  unbounce_phone,
	    right(decrypt_mobile_string(lead_phone),10)::varchar AS  unbounce_phone_decrypted,
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
	WHERE  lower(source) in (select distinct lower(affiliate) from affiliates)
	AND lead_arrival_time::date >= '2019-01-01'
	AND len(lead_phone) >= 10
	AND data_source <>'dm_missing_leads_db_dr'
),




non_ga AS 
(
select  * from webflow_ga 
where lower(source) in ('1webflow', '3chakra')
union 
select  * from dm_leads 
union
select * from all_leads_funnel_dm
where conc not in (
					select distinct conc from webflow_ga where lower(source)  in ('1webflow', '3chakra')
					union 
					select distinct conc from dm_leads 
					)
)

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
	    case when lower(utmSource) = 'digital-journey' then 'google' else lower(utmSource) end as utmSource,
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
from (
select distinct *, row_number() over(partition by unbounce_phone,lead_date order by lead_timestamp, source asc) as row_numb2,
		
	    max((case when cta_id is null or cta_id in ('', '-') THEN -1 ELSE cta_id::bigint END)::float) over(partition by unbounce_phone, lead_date, utmsource) AS max_cta_id
from
	(
	SELECT * from non_ga
	union
	select * from webflow_ga 
	where lower(source) = '1ga' 
		and conc not in (select distinct conc from non_ga)
	)
	)
where row_numb2 = 1 and lead_date < getdate()::date

;