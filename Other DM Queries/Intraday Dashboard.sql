--TRUNCATE TABLE dm.daily_intraday_leads;

--INSERT INTO dm.daily_intraday_leads

with
forms_data AS
(
select distinct
json_extract_path_text(data, 'id',TRUE) sessionId,
json_extract_path_text(data, 'applicant_name',TRUE)            name,
created_at::timestamp AS lead_timestamp,
json_extract_path_text(data, 'gclid',TRUE)            gclid,
json_extract_path_text(data, 'otp_verified_phone',TRUE) ::varchar           otp_verified_phone,
phone_number::varchar  as            unbounce_phone,
phone_number_decrypted::varchar  as            unbounce_phone_decrypted,
json_extract_path_text(json_extract_path_text(data, 'city',TRUE),'value',TRUE)            city,
json_extract_path_text(data, 'scheme_name',TRUE)            scheme_name,
json_extract_path_text(data, 'loan_amount',TRUE)            loan_amount,
json_extract_path_text(data, 'google_coordinates',TRUE)            google_coordinates,
json_extract_path_text(data, 'google_address',TRUE)            google_address,
json_extract_path_text(data, 'building_details',TRUE)            building_details,
json_extract_path_text(data, 'street_name',TRUE)            street_name,
json_extract_path_text(data, 'landmark',TRUE)            landmark,
json_extract_path_text(data, 'loanstartingtime',TRUE)            loanstartingtime,
json_extract_path_text(data, 'loanendtime',TRUE)            loanendtime,
json_extract_path_text(data, 'otp_verified',TRUE)            otp_verified,
case when json_extract_path_text(data, 'utm_source',TRUE) = '' and lower(json_extract_path_text(data, 'unbounce_url',TRUE)) not like '%rupeek web%'  then lead_source when lower(lead_source) in ('sms','smsrq','airtel','email') then 'SMS' else         json_extract_path_text(data, 'utm_source',TRUE) end             utmSource,
json_extract_path_text(data, 'utm_medium',TRUE)            utmMedium,
json_extract_path_text(data, 'utm_campaign',TRUE)            utmCampaign,
json_extract_path_text(data, 'applicant_name',TRUE)            applicant_name,
json_extract_path_text(data, 'weight',TRUE)            weight,
json_extract_path_text(data, 'quality',TRUE)            quality,
json_extract_path_text(data, 'flag_wt_qa',TRUE)            flag_wt_qa,
json_extract_path_text(data, 'unbounce_url',TRUE)            unbounceUrl,
json_extract_path_text(data, 'email',TRUE)            email,
json_extract_path_text(data, 'dm_gold_amount',TRUE)            dm_gold_amount,
json_extract_path_text(data, 'otp_verified_user',TRUE)            otp_verified_user,
json_extract_path_text(data, 'pin_code',TRUE)            pin_code,
json_extract_path_text(data, 'existing_loan',TRUE)            existing_loan,
json_extract_path_text(data, 'cta_id',TRUE)            cta_id,
created_at::date as lead_date,
concat(phone_number::varchar ,created_at::date) as conc,
'3chakra'::varchar as source
from dw.chakra_leads_fresh
where (lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%facebook_form%'  or lower(json_extract_path_text(data, 'bucket_type',TRUE)) like '%google_form%'
        or lower(lead_source) like '%google%' or lower(lead_source) like '%facebook%' or lower(data) like '%rupeek web%'
        or lower(lead_source) in ('affipedia', 'yoads', 'intellactads', 'digital-journey')
        or  lower(lead_source) in ('app', 'sms', 'smsrq', 'airtel','email')
        )
and created_at::date between getdate()::date-36 and getdate()::date and len(phone_number) >= 10
and lower(city) not in ('othercities')
),

dm_leads AS
(select DISTINCT sessionId,
            name,
            timestamp::timestamp AS lead_timestamp,
            gclid,
            otp_verified_phone::varchar,
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
            CASE when lower(unbounceurl) like '%rupeek web%' then 'website'::varchar 
            when lower(utmsource) in ('sms','smsrq','airtel','email') then 'SMS' ELSE utmSource::VARCHAR END AS utmSource,
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
            timestamp :: date between getdate()::date-36 and getdate()::date
            AND (
                lower(utmSource) like '%goog%'
                or lower(utmSource) like '%facebook%'
                or lower(utmSource) in ('sms', 'smsrq', 'airtel','email')
                or lower(utmSource) in ('google', 'facebook', 'fb', 'intellactads','affipedia', 'yoads')
                or lower(unbounceurl) like '%rupeek web%'
            )
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
            otp_verified_phone::varchar,
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
            timestamp :: date between getdate()::date-36 and getdate()::date
            AND (
                lower(utmSource) like '%goog%'
                or lower(utmSource) like '%facebook%'
                or lower(utmSource) in ('google', 'facebook', 'fb', 'intellactads','affipedia', 'yoads')
                or lower(unbounceurl) like '%rupeek web%'
            )
            
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
            case when lower(source) in ('sms','smsrq','airtel','email') then 'SMS'else source::varchar end AS utmSource,
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
        WHERE (lower(source) like '%goog%'
                        OR lower(source) in ('google' , 'intellactads','affipedia', 'yoads')
                        or lower(source) in ('sms', 'smsrq', 'app', 'airtel')
                        OR lower(source) like '%fb%'
                        OR lower(source) LIKE '%acebook%'
                        OR lower(source) like '%web%'
                        OR lower(source) like '%you%'
                        OR lower(source) like '%generic%'
                        OR lower(source) like '%brand%'
                        OR lower(source) like '%website%' )
                AND lead_arrival_time::date between getdate()::date-36 and getdate()::date
                AND len(lead_phone) >= 10
                AND data_source <>'dm_missing_leads_db_dr'
                --and lower(city) not in ('othercities') 
),


Referral_leads_dm AS (
SELECT DISTINCT
        a.id::VARCHAR AS sessionId,
        referralcode::VARCHAR AS name,
        a.created_at::timestamp AS lead_timestamp,
        '' AS gclid,
        '' AS otp_verified_phone,
        phone::varchar AS  unbounce_phone,
        phone_decrypted::varchar AS  unbounce_phone_decrypted,
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
        'Refferal'::varchar AS utmSource,
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
        'Refferal_Leads'::varchar AS source
from dw.core_referral a
    left join dw.core_user b on a.user_id = b.id
    left join dw.city c on c.city_id = b.cityid
    where lower(role) = 'customer'
    and a.created_at::date between getdate()::date-36 and getdate()::date
    and referralcode is not null
        AND len(phone) >= 10
        AND b.source <>'dm_missing_leads_db_dr'
),

App_leads_dm AS (
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
        'APP'::varchar AS utmSource,
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
from dw.core_customerloanrequest a
left join dw.core_user b on a.requested_id = b.id
    left join dw.city c on c.city_id = b.cityid
    where lower(role) = 'customer'
    and lower(city) not in ('othercities')
    and a.created_at::date between getdate()::date-36 and getdate()::date
        AND len(phone) >= 10
        --AND b.source <>'dm_missing_leads_db_dr'
),





non_ga AS
(
select  *,2::int as priority from webflow_ga
where lower(source) in ('1webflow', '3chakra')
union
select  *,2::int as priority from dm_leads
union
select *,2::int as priority from all_leads_funnel_dm
union
select *,1::int as priority from Referral_leads_dm
union
select *,2::int as priority from App_leads_dm
where conc not in (
                                        select distinct conc from webflow_ga where lower(source)  in ('1webflow', '3chakra')
                                        union
                                        select distinct conc from dm_leads
                                        )
),

dm_All_leads as
(
    select
    lead_timestamp,
    unbounce_phone,
    unbounce_phone_decrypted,
    unbounceurl,
    city,
    otp_verified,
    case when lower(utmSource) = 'digital-journey' then 'google' else utmSource end as utmSource,
    utmMedium,
    case when lower(utmSource) = 'digital-journey' then 'digital-journey' else utmCampaign end as utmCampaign,
    cta_id,
    lead_date,
    max_cta_id
    from (
    select distinct *, row_number() over(partition by unbounce_phone,lead_date order by lead_timestamp asc, priority asc) as row_numb2,
                   
                max((case when cta_id is null or cta_id in ('', '-') THEN -1 ELSE cta_id::bigint END)::float) over(partition by unbounce_phone, lead_date, utmsource) AS max_cta_id
    from
            (
            SELECT * from non_ga
            union
            select *,2::int as priority from webflow_ga
            where lower(source) = '1ga'
                    and conc not in (select distinct conc from non_ga)
            )
            )
    where row_numb2 = 1 and lead_date <= getdate()::date
),

mapped_leads AS (

select A.*,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.campaign_name END AS campaign_name,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.bucket1 END AS campaign_bucket,
B.city AS city_abr,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.channel_group END AS channel_group,
B.city_group AS city_group,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.bucket END AS sem_bau_bucket,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.type END AS type,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.channel_grouping_1 
 END AS channel_grouping1,
B.sem_campaigns_buckets AS bau_sub_buckets
from dm_all_leads A
left join (
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
                 WHERE row_num = 1 and utm_campaign__webflow_ <> '' and utm_campaign__webflow_ is not null) AS B
ON lower(A.utmcampaign) = lower(B.utm_campaign__webflow_)
WHERE unbounce_phone is not null and unbounce_phone <> '' and len(unbounce_phone) >= 10 
             /*concat(right(unbounce_phone::varchar,10),lead_date::date) not in 
                                                                (select distinct concat(unbounce_phone::varchar,lead_date::date) from 
                                                                dm.dr_mapped_leads_jan_apr */
                                                                        /*where (
                                                                                lower(utmSource) like '%goog%'
                                                                                or lower(utmSource) like '%facebook%'
                                                                                or lower(utmSource) in ('google', 'facebook', 'fb', 'intellactads','affipedia', 'yoads')
                                                                                or lower(unbounceurl) = 'rupeek web'
                                                                                                )
                                            )*/
),

intraday as
(
    select count(unbounce_phone) as Leads,lead_timestamp::date as Lead_date,extract ( hour from lead_timestamp) as hr 
    , extract(dow from lead_timestamp::date) as Dow
    , case when lower(utmsource) in ('app','referral','sms') then upper(utmsource)
      when upper(channel_group) = 'NEW CHANNEL' then 'AFFILIATES' else upper(channel_group) end as channel_group
    , case when lower(utmsource) in ('app','referral','sms') or upper(channel_group) = 'NEW CHANNEL' then upper(utmsource) else upper(sem_bau_bucket) end as Sub_Bucket
    , case when lower(utmsource) in ('app','referral','sms') or upper(channel_group) = 'NEW CHANNEL' then b.abr2 else city_abr end as City
    from mapped_leads a
    left join temp.dm_cities b on lower(a.city) = lower(b.city) 
    group by 2,3,4,5,6,7
) 

-- select lead_date as LeadDate
-- , hr as Hour
-- , to_char(lead_date, 'Dy') as DoW
-- , case when sub_bucket = 'FACEBOOK' and channel_group not like '%ATL%' then 'FACEBOOK'
-- when channel_group in ('GDN/DISCOVERY - REACH', 'SEM - EXPERIMENTS') then 'OTHERS'
-- when channel_group = 'REMARKETING' and sub_bucket = 'DISPLAY' then 'OTHERS'
-- else channel_group end as Channels
-- , Sub_Bucket
-- , UPPER(city) as City
-- , sum(leads) as Leads
-- from intraday
-- group by 1,2,3,4,5,6

select * from dm_all_leads
where lead_date between getdate()::date - 8 and getdate()::date

;