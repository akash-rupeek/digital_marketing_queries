TRUNCATE TABLE  dm.dr_affiliates_mapped_leads_v2;

INSERT INTO dm.dr_affiliates_mapped_leads_v2

-- CREATE TABLE dm.dr_affiliates_mapped_leads_v2 AS

with min_txn_date AS 
(
    select mobile_num as mobile_num, min(date) as min_txn_date 
    from growth.rpt_delivery_dashboard_v1 where lower(status) = 'completed' group by 1
),


mapped_leads AS 
(
    select A.*,
    utmcampaign AS campaign_name,
    'Digital Affliate'::varchar AS campaign_bucket,
    C.city_ab AS city_abr,
    'New Channel'::varchar AS channel_group,
    ''::varchar AS city_group,
    utmsource AS sem_bau_bucket,
    'Affiliates'::varchar AS type,
    'Digital Affiliates'::varchar AS channel_grouping1,
    utmsource AS bau_sub_buckets
    from dm.dr_affiliates_leads_raw_leads_v2 A
    left join dm.city_mapping C
    on lower(A.city) = lower(C.city)
    WHERE unbounce_phone is not null and unbounce_phone <> '' and len(unbounce_phone) >= 10
)


select *,
CASE WHEN max_cta_id in (2)  THEN 1 ELSE 0 END AS qualified_leads

from 
(
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
    campaign_name,
    campaign_bucket,
    CASE WHEN (city_abr is NULL or city_abr in ('',' ','#N/A','0',0)) THEN coalesce(city_abr, 'Other') ELSE city_abr END city_abr,
    channel_group,
    city_group,
    sem_bau_bucket,
    type,
    channel_grouping1,
    bau_sub_buckets,
    CASE WHEN B.min_txn_date is null then 'New' ELSE 'Repeat' END AS customer_type,
    max_cta_id

    FROM mapped_leads A

    LEFT JOIN min_txn_date AS B

    on A.unbounce_phone::varchar = B.mobile_num::varchar
    and A.lead_date > B.min_txn_date
)

where channel_group is not null and channel_group <> '' and channel_group <> 0 and len(unbounce_phone) >= 10 

;