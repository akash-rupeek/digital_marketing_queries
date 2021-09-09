TRUNCATE TABLE dm.dm_missing_leads_db_dr;

INSERT INTO dm.dm_missing_leads_db_dr 
with dm_leads AS
(select distinct unbounce_phone as lead_phone
                , "timestamp" AS lead_arrival_time
                , date as lead_date
                , lower(city) as city
                , utmsource
                , utmcampaign
                , conc
                , 'dmleads' as source
FROM 
        (
        SELECT DISTINCT
            sessionId,
            REPLACE(name, ',', '') AS name,
            timestamp AS timestamp,
            REPLACE(gclid, ',', '') AS gclid,
            REPLACE(otp_verified_phone, ',', '') AS otp_verified_phone,
            REPLACE(unbounce_phone, ',', '') AS unbounce_phone,
            REPLACE(city, ',', '') AS city,
            REPLACE(scheme_name, ',', '') AS scheme_name,
            REPLACE(loan_amount, ',', '') AS loan_amount,
            REPLACE(google_coordinates, ',', '') AS google_coordinates,
            REPLACE(google_address, ',', '') AS google_address,
            REPLACE(building_details, ',', '') AS building_details,
            REPLACE(street_name, ',', '') AS street_name,
            REPLACE(landmark, ',', '') AS landmark,
            REPLACE(loanstartingtime, ',', '') AS loanstartingtime,
            REPLACE(loanendtime, ',', '') AS loanendtime,
            REPLACE(otp_verified, ',', '') AS otp_verified,
            CASE when lower(unbounceurl) = 'rupeek web' then 'website' ELSE REPLACE(utmSource, ',', '') END AS utmSource,
            REPLACE(utmMedium, ',', '') AS utmMedium,
            REPLACE(utmCampaign, ',', '') AS utmCampaign,
            REPLACE(name, ',', '') AS applicant_name,
            REPLACE(weight, ',', '') AS weight,
            REPLACE(quality, ',', '') AS quality,
            REPLACE(flag_wt_qa, ',', '') AS flag_wt_qa,
            REPLACE(unbounceUrl, ',', '') AS unbounceUrl,
            REPLACE(email, ',', '') AS email,
            REPLACE(dm_gold_amount, ',', '') AS dm_gold_amount,
            REPLACE(otp_verified_user, ',', '') AS otp_verified_user,
            REPLACE(pin_code, ',', '') AS pin_code,
            REPLACE(existing_loan, ',', '') AS existing_loan,
            REPLACE(cta_id, ',', '') AS cta_id,
            timestamp :: date AS date
            , ROW_NUMBER() OVER(PARTITION BY unbounce_phone, "timestamp"::date ORDER by "timestamp", utmcampaign ASC) AS row_numb
                        , concat(unbounce_phone,timestamp::date) as conc
        from
            growth.dm_leads
        WHERE
            timestamp :: date BETWEEN '2020-01-01'
            AND getdate() -1
            AND (
                lower(utmSource) like '%goog%'
                or lower(utmSource) like '%facebook%'
                or lower(utmSource) in ('google', 'facebook', 'fb','affipedia', 'yoads', 'intellactads')
                or lower(unbounceurl) like '%rupeek web%'
            )
            AND "timestamp"::date >= '2020-01-01'
                AND len(unbounce_phone) >= 10
        )
WHERE row_numb = 1
),


webflow_ga AS
(SELECT DISTINCT
            sessionId,
            name,
            timestamp::timestamp AS timestamp,
            gclid,
            otp_verified_phone,
            unbounce_phone,
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
            utmSource,
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
            date,
            conc,
                source,
                unbounce_phone_decrypted
FROM 
        (
        SELECT DISTINCT
            sessionId,
            name,
            lead_timestamp::timestamp AS timestamp,
            gclid,
            otp_verified_phone,
            unbounce_phone,
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
            CASE when lower(unbounceurl) like '%rupeek web%' then 'website' ELSE utmSource END AS utmSource,
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
            lead_timestamp :: date AS date
            , ROW_NUMBER() OVER(PARTITION BY unbounce_phone, "lead_timestamp"::date ORDER by "lead_timestamp", utmcampaign ASC) AS row_numb
                , concat(unbounce_phone,lead_timestamp::date) as conc
                ,source
                ,unbounce_phone_decrypted
        from
            dm.dr_google_fb_leads
        WHERE
            lead_timestamp :: date BETWEEN '2020-01-01'
            AND getdate() -1
            /*AND (
                lower(utmSource) like '%goog%'
                or lower(utmSource) like '%facebook%'
                or lower(utmSource) in ('affipedia', 'yoads','google', 'facebook', 'fb', 'intellactads')
                or lower(unbounceurl) like '%rupeek web%'
            )*/
                AND len(unbounce_phone) >= 10
        )
WHERE row_numb = 1
),

all_leads_funnel_dm AS (
SELECT distinct lead_phone
                ,lead_arrival_time
                ,lead_date
                ,city
                ,utmsource
                ,utmcampaign
                ,conc
                ,data_source
                , 'all_leads' AS source
FROM
        (
        select distinct lead_phone
                        , lead_arrival_time
                        , lead_arrival_time::date as lead_date
                        , lower(city_growth) AS city
                        , source AS utmsource
                        , campaign AS utmcampaign
                        , ROW_NUMBER() OVER(PARTITION BY lead_phone, lead_date ORDER by lead_arrival_time, utmcampaign ASC) AS row_numb
                        , concat(lead_phone,lead_arrival_time::date) as conc
                        , data_source
        from growth.stg_all_leads_funnel
        WHERE (lower(source) like '%goog%' 
                        OR lower(source) in ('affipedia', 'yoads','google' , 'intellactads')
                        OR lower(source) like '%fb%' 
                        OR lower(source) LIKE '%acebook%' 
                        OR lower(source) like '%web%' 
                        OR lower(source) like '%you%' 
                        OR lower(source) like '%generic%' 
                        OR lower(source) like '%brand%' )
                AND lead_arrival_time::date >= '2020-01-01'
                AND len(lead_phone) >= 10
        )
WHERE row_numb = 1
)


select * from webflow_ga 
where conc not in (select distinct conc from dm_leads)

;