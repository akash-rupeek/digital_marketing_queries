with vars as
(
    select 
    '2020-12-01' as start_date
    -- '2021-04-01' as start_date
    , current_date - INTEGER '1' AS end_date
    --, '2021-04-04' as end_date
),

top_funnel as
(
    select ad_campaign_id
    , campaign_date, campaign_name
    , sum(impressions) as impressions
    , sum(clicks) as clicks
    , sum(spends) as spends
    from
    (
        select 
        ad_id
        , ad_g_id
        , ad_campaign_id
        
        , campaign_date
        , ad_campaign_name as campaign_name
        , sum(impressions) as impressions   
        , sum(clicks) as clicks
        , sum(spends) as spends
        from 

            (
                select ad_group_id
                , ad_id
                , name
                , clicks
                , impressions   
                , spends    
                , "date" as campaign_date 
                from dm.google_ads
            )a


            left join 


            (
                SELECT 
                ad_g_id
                , ad_group_name
                , ad_group_label
                , ad_campaign_id
                , ad_campaign_name
                , ad_campaign_label
                , "status"
                from 
                
                (
                    (
                        select id as ad_g_id
                        , "name" as ad_group_name
                        , labels as ad_group_label  
                        , campaign_id
                        from dm.google_ad_group
                    ) a 

                    left join 

                    (
                        select id as ad_campaign_id 
                        , "name" as ad_campaign_name    
                        , labels as ad_campaign_label   
                        , "status" --jaswanth to provide status codes
                         from dm.google_campaign
                     ) b 

                    on a.campaign_id = b.ad_campaign_id
                )c
            )adgc 
             
        on a.ad_group_id = adgc.ad_g_id
        group by 
        ad_id
        , ad_g_id
        , ad_campaign_id
        , campaign_date
        , ad_campaign_name
    ) zz
    where campaign_date between (select start_date from vars) and (select end_date from vars)
    group by
    ad_campaign_id
    , campaign_date, campaign_name
),

ctc_completion_data as
(
    select left(campaign, len(campaign)-14) as campaign
    , right(campaign, 11) as campaign_id
    , to_date(date1, 'YYYYMMDD') as date1
    , ctc_completions
    from temp.ctc_completion2
),

exp_list as
(
    select lower(city) as city, campaign_type, campaign_name, did_number, campaign_id
    from temp.ctc_experiments_list3
    group by 1,2,3,4,5
),

top_funnel_agg as
(
    select 
    ad_campaign_id
    , campaign_date, tf.campaign_name, city, campaign_type
    , impressions, clicks, spends
    from
    (
        select 
        ad_campaign_id
        , campaign_date, campaign_name
        , sum(impressions) as impressions
        , sum(clicks) as clicks
        , sum(spends) as spends
        from top_funnel
        where lower(campaign_name) in (select distinct lower(campaign_name) from exp_list)
        group by 
        ad_campaign_id
        , campaign_date, campaign_name
    ) tf
    left join exp_list
    on lower(tf.campaign_name) = lower(exp_list.campaign_name)
    where campaign_type is not null
),

top_ctc as
(
    select campaign_date, city, campaign_type
    , sum(impressions) impressions
    , sum(clicks) as clicks
    , sum(spends) as spends
    , sum(ctc_completions) as ctc_completions
    from
    (
        select tfagg.*, ctcdata.ctc_completions
        from top_funnel_agg tfagg
        left join ctc_completion_data ctcdata
        on tfagg.ad_campaign_id = ctcdata.campaign_id and tfagg.campaign_date = ctcdata.date1
    ) tfagctc
    group by campaign_date, city, campaign_type
),

inbound as
(
    select cz.mobile::varchar as mobile
    , cz.lead_date::date as lead_date
    , 'did_number_' + exp_list.did_number::varchar as campaign_name
    , 'did_number_' + exp_list.did_number::varchar as campaign_id
    , 'Test'::varchar as campaign_type
    , city::varchar as city
    from
    (
        select distinct right(customer_ph_no_decrypted,10) as mobile, date_time::DATE as lead_date, did_number
        from dw.c_zentrix_calls
        where date_time::DATE between (select start_date from vars) and (select end_date from vars)
        and did_number in (select distinct cast(did_number as varchar) from exp_list)
        and lower(orientation_type) = 'inbound'
        order by date_time::DATE, customer_ph_no
    ) cz
    left join exp_list
    on cz.did_number = cast(exp_list.did_number as varchar)
    group by 1,2,3,4,5,6
),

dm_leads as 
(
    select dml.unbounce_phone_decrypted::varchar as mobile
    , dml.date1::date as lead_date
    , exp_list.campaign_name::varchar as campaign_name
    , exp_list.campaign_id::varchar as campaign_id
    , exp_list.campaign_type::varchar as campaign_type
    , exp_list.city::varchar as city
    from
    (
        select unbounce_phone_decrypted, date1, utmcampaign, city
        from
        (
            select distinct right(unbounce_phone_decrypted,10) as unbounce_phone_decrypted
            , "timestamp"::DATE as date1
            , utmcampaign
            , lower(city) as city
            from growth.dm_leads
            where "timestamp"::DATE between (select start_date from vars) and (select end_date from vars)
            --and lower(utmsource) = 'google'
            and utmcampaign in (select distinct cast(campaign_id as varchar) from exp_list)

            union all

            select distinct right(lead_phone_decrypted,10) as lead_phone_decrypted
            , created_at::DATE as date1
            , utm_campaign
            , lower(city) as city
            from growth.dm_internal_leads
            where created_at::DATE between (select start_date from vars) and (select end_date from vars)
            --and lower(utmsource) = 'google'
            and utm_campaign in (select distinct cast(campaign_id as varchar) from exp_list) 
        ) dml_temp
        group by 1,2,3,4
    ) dml
    left join exp_list
    on dml.utmcampaign = cast(exp_list.campaign_id as VARCHAR)
    where exp_list.campaign_type is not null
    group by 1,2,3,4,5,6
),

all_leads as
(
    select mobile::varchar
    , lead_date
    , city
    , count(distinct case when campaign_type = 'Test' then mobile else null end) as test_lead
    , count(distinct case when campaign_type = 'Control' then mobile else null end) as control_lead
    , count(distinct case when call_type = 'inbound' then mobile else null end) as inbound_leads
    , count(distinct case when call_type = 'outbound' then mobile else null end) as outbound_leads
    from
    (
        select *, 'inbound'::text call_type from inbound
        union all
        select *, 'outbound'::text as call_type from dm_leads
    ) leads_temp
    group by 1,2,3
),

sch_data_non_ab as 
(
    select distinct timestamp::date AS date,
    (case when len(mobile_number_decrypted) < 14 then right(mobile_number_decrypted::varchar,10)  
    when len(mobile_number_decrypted) >= 14 then LEFT(mobile_number_decrypted::varchar,10)  END)::varchar
    AS mobile_number,        
    lower(transaction_type) AS txn_type
    from growth.gs_txn_req_all_cities_comb 
    where  timestamp::date between (select start_date from vars) and (select end_date from vars)
    AND lower(status) = 'scheduled' and len(mobile_number_decrypted) >9 and len(mobile_number_decrypted) < 20
),

sch_data_ab as 
(
    select distinct timestamp::date AS date,
    (LEFT(mobile_number_decrypted::varchar,10))::varchar AS mobile_number,        
    (right(mobile_number_decrypted::varchar,10))::varchar  AS a_b_mobile,
    lower(transaction_type) AS txn_type
    from growth.gs_txn_req_all_cities_comb 
    where  timestamp::date between (select start_date from vars) and (select end_date from vars)
    AND lower(status) = 'scheduled' and len(mobile_number_decrypted) >= 20
),

leads as (select distinct mobile::varchar as unbounce_phone, lead_date from all_leads),

txn_data_ab AS 
(
    SELECT (replace(mobile_num_decrypted,',',''))::varchar AS mobile_number
    , (replace(a_b_mobile_decrypted,',',''))::varchar AS a_b_mobile
    , date::date as date
    , date::date as created_at
    , SUM(REPLACE(final_amount,',','')::int) as final_amount 
    , type AS txn_type
    from growth.gs_txn_teardown_new 
    where lower(status) = 'completed' 
    and REPLACE(final_amount,',','') != '9923333999' 
    and  final_amount is not null 
    AND date::date >='2020-01-01'
    AND a_b_mobile is not null 
    GROUP BY 1,2,3,4,6
),

txn_data_non_ab as 
(
    SELECT (replace(mobile_num_decrypted,',',''))::varchar AS mobile_number
    , (replace(a_b_mobile_decrypted,',',''))::varchar AS a_b_mobile
    , date::date as date
    , date::date as created_at
    , SUM(REPLACE(final_amount,',','')::int) as final_amount 
    , type AS txn_type
    from growth.gs_txn_teardown_new 
    where lower(status) = 'completed' 
    and REPLACE(final_amount,',','') != '9923333999' 
    and  final_amount is not null 
    AND date::date >='2020-01-01'
    AND a_b_mobile is null 
    GROUP BY 1,2,3,4,6
),

sch_mapped AS 
(
    select distinct unbounce_phone,
    date,
    txn_type,
    lead_date,
    1 AS sch
    FROM
    (
        select distinct unbounce_phone, date, txn_type, lead_date
        , row_number() over(partition by unbounce_phone, lead_date order by date asc) as row_num
        from 
        (
            select unbounce_phone,
            date,
            txn_type,
            max(lead_date) as lead_date
            FROM sch_data_ab
            JOIN leads
            ON (sch_data_ab.mobile_number = leads.unbounce_phone or sch_data_ab.a_b_mobile = leads.unbounce_phone)
            AND sch_data_ab.date >= leads.lead_date
            group by 1,2,3

            UNION 

            select unbounce_phone,
            date,
            txn_type,
            max(lead_date) as lead_date
            FROM sch_data_non_ab
            JOIN leads
            ON sch_data_non_ab.mobile_number = leads.unbounce_phone
            AND sch_data_non_ab.date >= leads.lead_date
            group by 1,2,3
        )
    ) 
    WHERE row_num = 1
),

min_txn_date AS 
(select mobile_num, min(date) as min_txn_date from growth.gs_txn_teardown_new where lower(status) = 'completed' group by 1),

txn_mapped AS 
(
    select distinct unbounce_phone,
    date,
    txn_type,
    lead_date, final_amount,
    1 AS txn
    FROM
    (
        select distinct unbounce_phone, date, txn_type, lead_date, final_amount
        , row_number() over(partition by unbounce_phone, lead_date order by date asc) as row_num
        from 
        (
            select unbounce_phone,
            date,
            txn_type,
            final_amount,
            max(lead_date) as lead_date
            FROM txn_data_ab
            JOIN leads
            ON (txn_data_ab.mobile_number = leads.unbounce_phone or txn_data_ab.a_b_mobile = leads.unbounce_phone)
            AND txn_data_ab.date >= leads.lead_date
            group by 1,2,3,4

            UNION 

            select unbounce_phone,
            date,
            txn_type,
            final_amount,
            max(lead_date) as lead_date
            FROM txn_data_non_ab
            JOIN leads
            ON txn_data_non_ab.mobile_number = leads.unbounce_phone
            AND txn_data_non_ab.date >= leads.lead_date
            group by 1,2,3,4
        )
    ) WHERE row_num = 1 
),

ml_funnel AS
(
    select A.*,
    B.txn_type as sch_type ,
    COALESCE(B.sch,0) AS sch,
    B.date AS sch_date,
    C.txn_type as txn_type ,
    COALESCE(C.txn,0) AS txn,
    C.date AS txn_date
    FROM
    (
        select * 
        from all_leads
        where mobile <> '-' 
        and mobile is not null 
        and len(mobile) >= 10 
        and lead_date >= '2020-01-01'
    ) AS A

    LEFT JOIN sch_mapped AS B 
    ON A.mobile::varchar = B.unbounce_phone::varchar ANd A.lead_date::date = B.lead_date::date
    LEFT JOIN txn_mapped AS C
    ON A.mobile::varchar = C.unbounce_phone::varchar ANd A.lead_date::date = C.lead_date::date
)

select * from ml_funnel
where mobile in ('8073375484', '9663226958', '9920944124')
;