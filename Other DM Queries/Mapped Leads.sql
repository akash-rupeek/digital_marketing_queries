TRUNCATE TABLE  dm.dr_mapped_leads;

insert into dm.dr_mapped_leads


with min_txn_date AS 
(select mobile_num as mobile_num, mobile_num_decrypted::varchar as mobile_num_decrypted, min(date) as min_txn_date 
from growth.gs_txn_teardown_new where lower(status) = 'completed' group by 1,2),


mapped_leads AS (

select A.*,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.campaign_name END AS campaign_name,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.bucket1 END AS campaign_bucket,
B.city AS city_abr,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.channel_group END AS channel_group,
B.city_group AS city_group,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.bucket END AS sem_bau_bucket,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.type END AS type,
CASE when (lower(unbounceurl) like '%rupeek web%' or lower(utmsource) = 'website') then 'Website'::varchar ELSE B.channel_grouping_1 END AS channel_grouping1,
B.sem_campaigns_buckets AS bau_sub_buckets
from dm.dr_google_fb_leads A
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
WHERE unbounce_phone is not null and unbounce_phone <> '' and len(unbounce_phone) >= 10 and 
             concat(right(unbounce_phone::varchar,10),lead_date::date) not in 
                                                                (select distinct concat(unbounce_phone::varchar,lead_date::date) from 
                                                                dm.dr_mapped_leads_jan_apr 
                                                                        where (
                                                                                lower(utmSource) like '%goog%'
                                                                                or lower(utmSource) like '%facebook%'
                                                                                or lower(utmSource) in ('google', 'facebook', 'fb', 'intellactads','affipedia', 'yoads')
                                                                                or lower(unbounceurl) = 'rupeek web'
                                                                                                )
                                            )
)


select *,
                CASE WHEN campaign_bucket like '%YT - Remarketing%' THEN 1
                WHEN campaign_bucket like '%GDN - Remarketing%' THEN 1
                WHEN campaign_bucket like '%Website%' THEN 1
                WHEN campaign_bucket like '%Facebook - Remarketing%' THEN 1
                WHEN campaign_bucket like '%SEM - RQ%' THEN 1
                WHEN campaign_bucket like '%SEM - BAU%' THEN 1
                WHEN campaign_bucket like '%GDN - RQ%' AND max_cta_id in (1,2)  THEN 1
                WHEN campaign_bucket like '%GDN - Reach%' AND max_cta_id in (1,2)  THEN 1
                WHEN campaign_bucket like '%Discovery - Reach%' AND max_cta_id in (1,2)  THEN 1
                WHEN campaign_bucket like '%Facebook - Reach%' AND max_cta_id in (2)  THEN 1
                WHEN campaign_bucket like '%YT - RQ%' AND max_cta_id in (2)  THEN 1
                WHEN campaign_bucket like '%YT - Performance%' AND max_cta_id in (2)  THEN 1
                WHEN campaign_bucket like '%Digital Affliate%' AND max_cta_id in (2)  THEN 1
                WHEN campaign_bucket like '%FB - RQ%' AND max_cta_id in (2)  THEN 1
                WHEN campaign_bucket like '%Facebook - ATL%' AND max_cta_id in (2)  THEN 1
                WHEN campaign_bucket like '%SEM - EXP%' AND max_cta_id in (2)  THEN 1
                WHEN campaign_bucket like '%YT - Reach%' AND max_cta_id in (2)  THEN 1 ELSE 0 END AS qualified_leads

from (
select sessionid,
name,
lead_timestamp,
gclid,
otp_verified_phone,
unbounce_phone,
unbounce_phone_decrypted,
A.city,
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
utmsource,
utmmedium,
utmcampaign,
applicant_name,
weight,
quality,
flag_wt_qa,
unbounceurl,
email,
dm_gold_amount,
otp_verified_user,
pin_code,
existing_loan,
cta_id,
lead_date,
conc,
source,
CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A')) THEN utmcampaign ELSE campaign_name END AS 
campaign_name,
        CASE WHEN (campaign_bucket is NULL or  campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(utmsource) like '%google%' THEN 'SEM - BAU'
                                                   WHEN (lower(utmsource) like '%facebook%' or lower(utmsource) like '%fb%') THEN 'Facebook - Reach'
                                                   WHEN lower(utmsource) like '%website%' THEN 'Website' END
                                )
                WHEN lower(campaign_bucket) like '%organic%' THEN 'Website'
                        ELSE campaign_bucket END AS 
campaign_bucket,
        CASE WHEN (city_abr is NULL or city_abr in ('',' ','#N/A','0',0)) THEN coalesce(cm.city_ab, 'Other') ELSE city_abr END 
city_abr,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(utmsource) like '%google%' THEN 'BAU'
                                                   WHEN (lower(utmsource) like '%facebook%' or lower(utmsource) like '%fb%') THEN 'Facebook - Reach'
                                                   WHEN lower(utmsource) like '%website%' THEN 'Website' END
                                )
                WHEN lower(campaign_bucket) like '%organic%' THEN 'Website'
                        ELSE channel_group END AS 
channel_group,
city_group,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A') or sem_bau_bucket in ('',' ','#N/A') or sem_bau_bucket is null)
                        THEN (CASE         WHEN lower(utmsource) like '%google%' THEN '0'
                                                   WHEN (lower(utmsource) like '%facebook%' or lower(utmsource) like '%fb%') THEN 'Facebook'
                                                   WHEN lower(utmsource) like '%website%' THEN 'Website' 
                                                   WHEN lower(campaign_bucket) like '%organic%' THEN 'Website'END
                                )
                WHEN lower(campaign_bucket) like '%organic%' THEN 'Website'
                WHEN lower(campaign_bucket) like '%website%' THEN 'Website'
                        ELSE sem_bau_bucket END AS 
sem_bau_bucket,
        CASE WHEN type is null then '0'         
        WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(utmsource) like '%google%' THEN 'BAU'
                                                   WHEN (lower(utmsource) like '%facebook%' or lower(utmsource) like '%fb%') THEN 'Experiment'
                                                   WHEN lower(utmsource) like '%website%' THEN 'Website' END
                                )
                WHEN lower(campaign_bucket) like '%organic%' THEN 'Website'
                WHEN type is null then '0'         
                        ELSE type END AS 
type,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(utmsource) like '%google%' THEN 'BAU'
                                                   WHEN (lower(utmsource) like '%facebook%' or lower(utmsource) like '%fb%') THEN 'Facebook - Reach'
                                                   WHEN lower(utmsource) like '%website%' THEN 'Website' END
                                )
                WHEN lower(campaign_bucket) like '%organic%' THEN 'Website'
                        ELSE channel_group END AS 
channel_grouping1,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A') or bau_sub_buckets in ('',' ','#N/A') or bau_sub_buckets is null)
                        THEN (CASE         WHEN lower(utmsource) like '%google%' THEN '0'
                                                   WHEN (lower(utmsource) like '%facebook%' or lower(utmsource) like '%fb%') THEN '0'
                                                   WHEN lower(utmsource) like '%website%' THEN '0' 
                                                   WHEN lower(campaign_bucket) like '%organic%' THEN '0'
                WHEN lower(campaign_bucket) like '%website%' THEN '0'
                                                   WHEN lower(campaign_bucket) like '%organic%' THEN '0'END
                                )
                WHEN lower(campaign_bucket) like '%organic%' THEN '0'
                WHEN lower(campaign_bucket) like '%website%' THEN '0'
                        ELSE bau_sub_buckets END AS 
bau_sub_buckets,
CASE WHEN B.min_txn_date is null then 'New' ELSE 'Repeat' END AS customer_type,
max_cta_id

FROM 
(
select * from mapped_leads 
union 
select sessionid,
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
utmsource,
utmmedium,
utmcampaign,
applicant_name,
weight,
quality,
flag_wt_qa,
unbounceurl,
email,
dm_gold_amount,
otp_verified_user,
pin_code,
existing_loan,
cta_id,
lead_date,

             concat(unbounce_phone::varchar,lead_date::date) as conc,
source,
case when cta_id is null or cta_id in ('','-','_') then -1 else cta_id::bigint END as max_cta_id,
campaign_name,
campaign_bucket,
city_abr,
channel_group,
city_group,
sem_bau_bucket,
type,
channel_grouping1,
bau_sub_buckets  from dm.dr_mapped_leads_jan_apr
) AS A
LEFT JOIN min_txn_date AS B
on A.unbounce_phone::varchar = B.mobile_num::varchar
and A.lead_date > B.min_txn_date
LEFT JOIN dm.city_mapping as cm
ON lower(A.city) = lower(cm.city)
)
where channel_group is not null and channel_group <> '' and channel_group <> 0 and len(unbounce_phone) >= 10 

;

