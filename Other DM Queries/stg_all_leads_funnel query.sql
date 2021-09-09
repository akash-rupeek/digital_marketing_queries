stg_all_leads_funnel=truncate growth.stg_all_leads_funnel;
insert into
    growth.stg_all_leads_funnel(	
    WITH leads_data AS(
        WITH leads AS(
            WITH app AS(
                SELECT
                    *
                FROM
                    growth.gs_app_request_dump
            ),
            promo AS(
                SELECT
                    *
                FROM
                    dw.core_user
                    LEFT JOIN dw.core_offerdiscount ON core_user.id = core_offerdiscount.user_id
            )
            SELECT
                primaryphonenumber AS lead_phone,
                app.timestamp :: TIMESTAMP AS lead_arrival_time,
                usercity AS city,CASE
                    WHEN "type" IS NOT NULL THEN 'offer'
                    WHEN "type" IS NULL
                    AND address IS NOT NULL THEN 'address'
                    WHEN "type" IS NULL
                    AND address IS NULL
                    AND loanstarttime IS NOT NULL THEN 'timeslot'
                    WHEN "type" IS NULL
                    AND loanstarttime IS NULL
                    AND address IS NULL
                    AND primaryphonenumber IS NOT NULL THEN 'phone'
                    ELSE NULL
                END AS stage,
                'app' AS source,
                NULL AS campaign,
                'app_request_dump' AS data_source
            FROM
                app
                LEFT JOIN promo ON app.primaryphonenumber = promo.phone
            WHERE
                LENGTH("timestamp") > 10
            UNION
            SELECT
                mobile_number AS lead_phone,
                time_submitted :: TIMESTAMP AS lead_arrival_time,
                city,
                NULL AS stage,
                utm_source AS source,
                utm_campaign AS campaign,
                'adwords_dump' AS data_source
            FROM
                growth.gs_adwords_dump
            union
            SELECT
                mobile_number AS lead_phone,
                time_submitted :: TIMESTAMP AS lead_arrival_time,
                city,
                NULL AS stage,
                utm_source AS source,
                utm_campaign AS campaign,
                'adwords_lead_dump' AS data_source
            FROM
                growth.gs_adwords_dump
            WHERE
                date_submitted <> 'date_submitted'
            UNION
            SELECT
                customer_phone_number AS lead_phone,
                "timestamp" :: TIMESTAMP AS lead_arrival_time,
                city,
                NULL AS stage,CASE
                    WHEN LOWER(source_of_lead) = 'website' THEN 'website'
                    WHEN LOWER(source_of_lead) = 'new customer (support inbound)' THEN 'support'
                    WHEN LOWER(source_of_lead) = 'existing customer (support inbound)' THEN 'support'
                END AS source,
                NULL AS campaign,
                'digital_customer_acquisition' AS data_source
            FROM
                growth.digital_customer_acquisition
            WHERE
                LOWER(source_of_lead) IN(
                    'website',
                    'new customer (support inbound)',
                    'existing customer (support inbound)'
                )
            UNION
            SELECT
                unbounce_phone AS lead_phone,
                TIMESTAMP :: TIMESTAMP AS lead_arrival_time,
                city,CASE
                    WHEN otp_verified = 'Verified' THEN 'OTP Verified'
                    WHEN loanstartingtime IS NOT NULL THEN 'Timeslot'
                    WHEN building_details IS NOT NULL THEN 'Address'
                    WHEN scheme_name IS NOT NULL THEN 'Scheme'
                    ELSE 'Phone'
                END AS stage,CASE
                    WHEN LOWER(unbounceurl) LIKE '%%rupeek web%%' THEN 'website'
                    ELSE utmsource
                END AS source,
                utmcampaign AS campaign,
                'Dm_leads' AS data_source
            FROM
                growth.dm_leads AS web
            WHERE
                TIMESTAMP is not null
            UNION
            SELECT
                unbounce_phone AS lead_phone,
                TIMESTAMP :: TIMESTAMP AS lead_arrival_time,
                city,CASE
                    WHEN otp_verified = 'Verified' THEN 'OTP Verified'
                    WHEN loanstartingtime IS NOT NULL THEN 'Timeslot'
                    WHEN building_details IS NOT NULL THEN 'Address'
                    WHEN scheme_name IS NOT NULL THEN 'Scheme'
                    ELSE 'Phone'
                END AS stage,CASE
                    WHEN LOWER(unbounceurl) LIKE '%%rupeek web%%' THEN 'website'
                    ELSE utmsource
                END AS source,
                utmcampaign AS campaign,
                'dm_missing_leads_db_dr' AS data_source
            FROM
                dm.dm_missing_leads_db_dr AS web
            WHERE
                TIMESTAMP is not null
            UNION
            SELECT
                lead_phone,
                lead_arrival_time :: TIMESTAMP,CASE
                    WHEN city IS NULL
                    OR city = '' THEN campaign_name
                    ELSE city
                END AS city,
                NULL AS stage,CASE
                    WHEN did_type IS NULL
                    OR did_type = '' THEN 'support'
                    ELSE did_type
                END AS source,
                NULL AS utm_campaign,
                'czentrix_call_log' AS data_source
            FROM(
                    SELECT
                        customer_ph_no AS lead_phone,
                        date_time :: TIMESTAMP AS lead_arrival_time,
                        campaign_name AS campaign_name,
                        did_number AS did_num,
                        transferred_from
                    FROM
                        dw.c_zentrix_calls
                    WHERE
                        orientation_type = 'INBOUND'
                        AND (
                            LOWER(campaign_name) LIKE '%%openers%%'
                            OR LOWER(campaign_name) LIKE '%%ibfl%%'
                            OR LOWER(campaign_name) LIKE '%%dca%%'
                        )
                        and (
                            LOWER(transferred_to) not like '%%support%%'
                            or transferred_to is null
                        )
                ) call
                LEFT JOIN growth.inbound_did_number AS did ON call.did_num = did.did_number
            WHERE
                did_num IN(
                    SELECT
                        did_number
                    FROM
                        growth.inbound_did_number
                )
                OR LOWER(transferred_from) LIKE '%%support%%'
        ),
        trans AS(
            SELECT
                DISTINCT phone,
                MIN(cashtransferred :: DATE) AS first_trans_date
            FROM
                dw.core_loanrequest
                INNER JOIN dw.core_user ON core_loanrequest.requesterid = core_user.id
            WHERE
                statuscode BETWEEN 3.5
                AND 8
            GROUP BY
                1
        )
        SELECT
            leads.*,CASE
                WHEN LEFT(RIGHT(lead_phone, 10), 1) >= '6'
                AND LENGTH(RIGHT(lead_phone, 10)) = 10 THEN RIGHT(lead_phone, 10)
                ELSE NULL
            END AS phone_growth,CASE
                WHEN LOWER(leads.city) LIKE '%%ahmedabad%%' THEN 'ahmedabad'
                WHEN LOWER(leads.city) LIKE '%%ahm%%' THEN 'ahmedabad'
                WHEN LOWER(leads.city) LIKE '%%bangalore%%' THEN 'bangalore'
                WHEN LOWER(leads.city) LIKE '%%bengaluru%%' THEN 'bangalore'
                WHEN LOWER(leads.city) LIKE '%%blr%%' THEN 'bangalore'
                WHEN LOWER(leads.city) LIKE '%%chennai%%' THEN 'chennai'
                WHEN LOWER(leads.city) LIKE '%%chn%%' THEN 'chennai'
                WHEN LOWER(leads.city) LIKE '%%coimbatore%%' THEN 'coimbatore'
                WHEN LOWER(leads.city) LIKE '%%cmb%%' THEN 'coimbatore'
                WHEN LOWER(leads.city) LIKE '%%delhi%%' THEN 'delhi'
                WHEN LOWER(leads.city) LIKE '%%hyderabad%%' THEN 'hyderabad'
                WHEN LOWER(leads.city) LIKE '%%hyd%%' THEN 'hyderabad'
                WHEN LOWER(leads.city) LIKE '%%jaipur%%' THEN 'jaipur'
                WHEN LOWER(leads.city) LIKE '%%mumbai%%' THEN 'mumbai'
                WHEN LOWER(leads.city) LIKE '%%mum%%' THEN 'mumbai'
                WHEN LOWER(leads.city) LIKE '%%pune%%' THEN 'pune'
                WHEN LOWER(leads.city) LIKE '%%surat%%' THEN 'surat'
                ELSE leads.city
            END AS city_growth,
            bucket AS campaign_growth,
            growth.gs_campaign_bucket.type as campaign_type,CASE
                WHEN lead_arrival_time :: DATE > first_trans_date :: DATE THEN 'Repeat'
                ELSE 'New'
            END AS customer_type
        FROM
            leads
            LEFT JOIN growth.gs_campaign_bucket ON LOWER(leads.campaign) = LOWER(gs_campaign_bucket.utm_campaign__webflow_)
            LEFT JOIN trans ON RIGHT(leads.lead_phone, 10) = trans.phone
    ),
    txn_req AS(
        SELECT
            mobile_number AS txn_req_phone,
            txn.timestamp :: TIMESTAMP AS pushed_date,
            form_filled_by AS agent_name,
            status,
            transaction_type,CASE
                WHEN LOWER(status) = 'scheduled' THEN txn.timestamp :: TIMESTAMP
            END AS scheduled_date,
            "date" :: DATE AS preferred_date,
            loan_amount as loan_amount,
            scheduled_for_date :: DATE
        FROM
            growth.gs_txn_req_all_cities_comb AS txn
    ),
    core_txn AS(
        SELECT
            DISTINCT phone,
            city as core_city,
            cashtransferred :: TIMESTAMP AS cashtransferred_date,
            lm.loan_amount as core_loan_amount,
            takeover
        FROM
            dw.core_loanrequest as cl
            left join(
                select
                    distinct id as id,
                    sum(loan_amount) as loan_amount
                from
                    dw.core_loanrequest_loan
                group by
                    1
            ) lm on lm.id = cl.id
            INNER JOIN dw.core_user ON cl.requesterid = core_user.id
        WHERE
            statuscode BETWEEN 3.5
            AND 8
    )
    SELECT
        *
    FROM
        leads_data
        LEFT JOIN txn_req ON leads_data.phone_growth = txn_req.txn_req_phone
        AND txn_req.pushed_date :: DATE >= leads_data.lead_arrival_time :: DATE
        LEFT JOIN core_txn ON txn_req.txn_req_phone = core_txn.phone
        AND core_txn.cashtransferred_date :: DATE >= txn_req.pushed_date :: DATE
)