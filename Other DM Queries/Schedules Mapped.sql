TRUNCATE TABLE dm.dr_mapped_schs;
INSERT INTO dm.dr_mapped_schs



with txns AS 
(
select distinct
        (teardown.mobile_num)::varchar as mobile_num,
        teardown.date
        from growth.gs_txn_teardown_new AS teardown
        where teardown.date::date between '2020-01-01' and getdate()-1
        and lower(teardown.status)='completed'
        and replace(teardown.final_amount,',','')::float < 100000000
        
),

schs_raw AS ( 
SELECT distinct mobile_num,
        date,
        city,
        txn_type,
        source,
        status,
        a_b_mobile,
        lead_source,
        lead_arrival_time,
        source_priority
FROM (
SELECT distinct mobile_num,
        date,
        city,
        txn_type,
        source,
        status,
        a_b_mobile,
        lead_source,
        lead_arrival_time,
        source_priority,
        prev_txn_date,
        row_number() over(Partition by mobile_num,txn_type, prev_txn_date Order by date ASC ) AS rownumb
        
FROM 
(
Select distinct A.*, coalesce(txns.date, '2020-01-01') AS prev_txn_date
                , row_number() over(Partition by A.mobile_num,A.txn_type, A.date Order by prev_txn_date DESC ) AS rownumb
FROM 
(
        select distinct 
        mobile_num::varchar as mobile_num,
        date,
        city,
        type as txn_type,
        source,
        status,
        a_b_mobile::varchar as a_b_mobile,
        lead_source,
        lead_arrival_time,
        source_priority
         from dm.dr_schs_attributed_to_dm

) AS A
LEFT JOIN txns 
ON A.mobile_num::varchar = txns.mobile_num::varchar
        AND A.date > txns.date

)
WHERE rownumb = 1)
WHERE rownumb = 1),



leads AS 
(
select distinct  unbounce_phone, 
        lead_date, 
        source,
        utmcampaign,
        campaign_name,
        campaign_bucket,
        channel_group,
        channel_grouping1,
        city_abr,
        city_group,
        sem_bau_bucket,
        type,
        bau_sub_buckets
from dm.dr_mapped_leads

),

min_txn_date AS 
(select (mobile_num)::varchar as mobile_num, min(date) as min_txn_date from growth.gs_txn_teardown_new where lower(status) = 'completed' group by 1),

schs_raw_mapped AS 
(
SELECT * from 
(
select distinct A.*, B.unbounce_phone, B.lead_date
        , case when B.unbounce_phone is null then 'not mapped' else 'mapped' end as DM_data
        , ROW_NUMBER() OVER(PARTITION BY A.mobile_num,A.date ORDER by COALESCE(B.lead_date,A.lead_arrival_time::date) DESC) AS row_numb
FROM schs_raw AS A
LEFT JOIN (select distinct unbounce_phone, lead_date from leads) AS B
on (A.mobile_num=B.unbounce_phone 
                or left(A.a_b_mobile,10) = B.unbounce_phone 
                or right(A.a_b_mobile,10) = B.unbounce_phone)
and A.date::Date>=B.lead_date::date
)
WHERE row_numb = 1
)

select A.*, 
        B.source as lead_source_table,
        utmcampaign,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A')) THEN 'Not Mapped' ELSE 'Mapped' END AS campaign_mapping_flag,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A')) THEN utmcampaign ELSE campaign_name END AS campaign_name,
        CASE WHEN (campaign_bucket is NULL or  campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(lead_source) like '%google%' THEN 'SEM - BAU'
                                                   WHEN (lower(lead_source) like '%facebook%' or lower(lead_source) like '%fb%') THEN 'Facebook - Reach'
                                                   WHEN lower(lead_source) like '%website%' THEN 'Website' END
                                )
                        ELSE campaign_bucket END AS campaign_bucket,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(lead_source) like '%google%' THEN 'BAU'
                                                   WHEN (lower(lead_source) like '%facebook%' or lower(lead_source) like '%fb%') THEN 'Facebook - Reach'
                                                   WHEN lower(lead_source) like '%website%' THEN 'Website' END
                                )
                        ELSE channel_group END AS channel_group,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(lead_source) like '%google%' THEN 'SEM - BAU'
                                                   WHEN (lower(lead_source) like '%facebook%' or lower(lead_source) like '%fb%') THEN 'Reach Channels'
                                                   WHEN lower(lead_source) like '%website%' THEN 'Website' END
                                )
                        ELSE channel_group END AS channel_grouping1,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(lead_source) like '%google%' THEN '0'
                                                   WHEN (lower(lead_source) like '%facebook%' or lower(lead_source) like '%fb%') THEN 'Facebook'
                                                   WHEN lower(lead_source) like '%website%' THEN 'Website' END
                                )
                        ELSE sem_bau_bucket END AS sem_bau_bucket,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(lead_source) like '%google%' THEN 'BAU'
                                                   WHEN (lower(lead_source) like '%facebook%' or lower(lead_source) like '%fb%') THEN 'Experiment'
                                                   WHEN lower(lead_source) like '%website%' THEN 'Website' END
                                )
                        ELSE B.type END AS type,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('',' ','#N/A'))
                        THEN (CASE         WHEN lower(lead_source) like '%google%' THEN '0'
                                                   WHEN (lower(lead_source) like '%facebook%' or lower(lead_source) like '%fb%') THEN NULL
                                                   WHEN lower(lead_source) like '%website%' THEN NULL END
                                )
                        WHEN bau_sub_buckets is null THEN '0' ELSE bau_sub_buckets END AS bau_sub_buckets,
        CASE WHEN (city_abr is NULL or city_abr in ('',' ','#N/A','0',0, 'Other')) THEN coalesce(cm.city_ab, 'Other') ELSE city_abr END city_abr,
        city_group,
        case when  C.min_txn_date is null then 'New' ELSE 'Repeat' END AS customer_type
FROM schs_raw_mapped AS A
LEFT JOIN leads AS B
ON A.unbounce_phone = B.unbounce_phone
AND A.lead_date = B.lead_date
LEFT JOIN min_txn_date AS C
ON A.mobile_num = C.mobile_num
and A.date > C.min_txn_date
LEFT JOIN dm.city_mapping as cm
ON lower(A.city) = lower(cm.city)
;