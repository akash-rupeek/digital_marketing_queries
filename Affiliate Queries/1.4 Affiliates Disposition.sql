TRUNCATE TABLE dm.dr_affiliates_leads_disposition;

INSERT INTO dm.dr_affiliates_leads_disposition

with temp2 as
(
    select distinct unbounce_phone 
    from dm.dr_affiliates_mapped_leads
    where len(unbounce_phone) = 10 
),


temp3 as
(
    SELECT unbounce_phone
    ,date as lead_date
    , (case when next_lead_date is null or datediff(day,lead_date,next_lead_date) > 6   then lead_date + 6 else next_lead_date end) as next_lead_date_call
    , (case when next_lead_date is null or datediff(day,lead_date,next_lead_date) > 60   then lead_date + 60 else next_lead_date end) as next_lead_date
    FROM
    (
        select distinct unbounce_phone,lead_date as date,  lead(lead_date) over(partition by unbounce_phone  order by date asc) as next_lead_date 
        from dm.dr_affiliates_mapped_leads
        where len(unbounce_phone) = 10 
    )
),

dispos_raw as 
(
    select  distinct a.mobile_num, a.call_date,CAST(CASE when p.priority IS NULL THEN '0' ELSE p.priority END AS INT) as priority, p.disposition, p.status, p.sub_disposition
    , agent_name
    from
    (
        SELECT  distinct date_time as call_date, customer_ph_no as mobile_num, cust_disposition as disposition, agent_name
        FROM dw.c_zentrix_calls as A
        JOIN temp2 as B on A.customer_ph_no = B.unbounce_phone
    ) as a 
    LEFT JOIN dm.disposition_priority_updated AS p ON LOWER (p.disposition) = LOWER (a.disposition)

    UNION

    select distinct a.mobile_num, a.call_date, CAST(CASE when p.priority IS NULL THEN '0' ELSE p.priority END AS INT) as priority, p.disposition, p.status, p.sub_disposition
    , agent_name
    from
    (
        SELECT DISTINCT timestamp::date as call_date,customer_phone_no as mobile_num, call_status as status, sub_disposition, form_filled_by AS agent_name
        FROM growth.gs_dca_update_res_chakra as A
        JOIN temp2 as B on A.customer_phone_no = B.unbounce_phone
    ) as a 
    LEFT JOIN dm.disposition_priority_updated AS p ON LOWER (p.disposition) = LOWER (a.sub_disposition)

    UNION

    select distinct a.mobile_num, a.call_date, CAST(CASE when p.priority IS NULL THEN '0' ELSE p.priority END AS INT) as priority, p.disposition, p.status, p.sub_disposition
    , agent_name
    from
    (
        SELECT DISTINCT call_time as call_date,modified_customer_phone as mobile_num, status, sub_disposition, form_filled_by AS agent_name
        FROM growth.rpt_daily_dispositions_dca as A
        JOIN temp2 as B on A.modified_customer_phone = B.unbounce_phone
    ) as a 
    LEFT JOIN dm.disposition_priority_updated AS p ON LOWER (p.disposition) = LOWER (a.sub_disposition)

    UNION

    select distinct a.mobile_num, a.call_date, CAST(CASE when p.priority IS NULL THEN '0' ELSE p.priority END AS INT) as priority, p.disposition, p.status, p.sub_disposition
    , agent_name
    from
    (
        SELECT distinct timestamp as call_date, cx_phone_no as mobile_num, sub_disposition, form_filled_by AS agent_name
        FROM growth.gs_dca_update_res as A
        JOIN temp2 as B on A.cx_phone_no = B.unbounce_phone
    ) as a 
    LEFT JOIN dm.disposition_priority_updated AS p ON LOWER (p.disposition) = LOWER (a.sub_disposition)

    UNION

    select distinct a.mobile_num, a.call_date, CAST(CASE when p.priority IS NULL THEN '0' ELSE p.priority END AS INT) as priority, p.disposition, p.status, p.sub_disposition
    , agent_name
    from
    (
        SELECT distinct A.created_at as call_date, l.phone_number as mobile_num, A.sub_disposition, ag.display_name AS agent_name
        FROM (select distinct record_id, assignedto, created_at, sub_deposition as sub_disposition from dw.chakra_calls) as A
        JOIN (select distinct record_id, phone_number from dw.chakra_leads_fresh) AS l on A.record_id = l.record_id
        JOIN (select distinct id, display_name from dw.chakra_users) AS ag on A.assignedto = ag.id
        JOIN temp2 as B on l.phone_number = B.unbounce_phone
    ) as a 
    LEFT JOIN dm.disposition_priority_updated AS p ON LOWER (p.disposition) = LOWER (a.sub_disposition)

),

dispos_mapped as 
(
    select unbounce_phone
    ,lead_date
    ,next_lead_date
    ,call_date
    ,priority        
    ,status          
    ,disposition     
    ,sub_disposition
    ,agent_name
    FROM temp3 as A
    Left join (select * from dispos_raw where call_date is not null) as B 
    ON A.unbounce_phone = B.mobile_num
    AND B.call_date between A.lead_date and A.next_lead_date
),


dispos as
(
    SELECT * 
    FROM
    (
        SELECT a.unbounce_phone AS lead_phone
        , a.lead_date::date as lead_date2
        , a.next_lead_date
        , a.Call_date       AS last_call_date               
        , a.priority        AS last_priority
        , a.status          AS last_status         
        , a.disposition     AS last_disposition    
        , a.sub_disposition AS last_sub_disposition
        , a.agent_name AS agent_name
        , p1.disposition     AS max_disposition    
        , p1.sub_disposition AS max_sub_disposition 
        , p2.disposition     AS max_disposition_sd    
        , p2.sub_disposition AS max_sub_disposition_sd  
        , p3.disposition     AS max_disposition_swk    
        , p3.sub_disposition AS max_sub_disposition_swk  
        , p4.disposition     AS max_disposition_7d    
        , p4.sub_disposition AS max_sub_disposition_7d      
        , p5.disposition     AS max_disposition_14d    
        , p5.sub_disposition AS max_sub_disposition_14d       
        , p6.disposition     AS max_disposition_30d    
        , p6.sub_disposition AS max_sub_disposition_30d    
        , p7.disposition     AS max_disposition_60d    
        , p7.sub_disposition AS max_sub_disposition_60d
        ,ROW_NUMBER() OVER(PARTITION BY a.unbounce_phone, a.lead_date order by Call_date ASC) as ROWnumb
        FROM dispos_mapped as a
        left join (SELECT unbounce_phone,lead_date
        , max(priority) as max_priority
        , max(case when lead_date:: date = call_date::date then priority else 0 end) max_priority_sd
        , max(case when call_date::date >= lead_date:: date AND 
        (datepart(week,lead_date:: date) = datepart(week,call_date::date) or call_date::date <= next_mon_date) then priority else 0 end) max_priority_swk
        , max(case when lead_date:: date+6 >= call_date::date  then priority else 0 end) max_priority_7d
        , max(case when lead_date:: date+13 >= call_date::date  then priority else 0 end) max_priority_14d
        , max(case when lead_date:: date+29 >= call_date::date  then priority else 0 end) max_priority_30d
        , max(case when lead_date:: date+59 >= call_date::date  then priority else 0 end) max_priority_60d
        from dispos_mapped
        LEFT JOIN (select distinct lead_date AS lead_date_m,max(lead_date) over(partition by datepart(week,lead_date))+1 AS next_mon_date from temp3 group by 1) AS next_mon
        ON dispos_mapped.lead_date = next_mon.lead_date_m
        group by 1,2) as b 
        on a.unbounce_phone = b.unbounce_phone
        AND a.lead_date = b.lead_date
        LEFT join dm.disposition_priority_updated AS p1 ON LOWER (p1.priority) = LOWER (b.max_priority)
        LEFT join dm.disposition_priority_updated AS p2 ON LOWER (p2.priority) = LOWER (b.max_priority_sd)
        LEFT join dm.disposition_priority_updated AS p3 ON LOWER (p3.priority) = LOWER (b.max_priority_swk)
        LEFT join dm.disposition_priority_updated AS p4 ON LOWER (p4.priority) = LOWER (b.max_priority_7d)
        LEFT join dm.disposition_priority_updated AS p5 ON LOWER (p5.priority) = LOWER (b.max_priority_14d)
        LEFT join dm.disposition_priority_updated AS p6 ON LOWER (p6.priority) = LOWER (b.max_priority_30d)
        LEFT join dm.disposition_priority_updated AS p7 ON LOWER (p7.priority) = LOWER (b.max_priority_60d)
    )
    where ROWnumb = 1
),

dm_leads as
(
    select unbounce_phone as unbounce_phone, lead_timestamp::date as lead_date, lead_timestamp lead_datetime
    from dm.dr_affiliates_mapped_leads 
    where lead_timestamp::date >= '2020-01-01'
    group by 1,2,3
),

call_data as 
(
    SELECT unbounce_phone
    ,lead_date
    ,next_lead_date_call

    ,lead_datetime
    , case when lead_date = min(date_time::date) then 'same day' else 'not same day' end as same_day_flag
    , datediff(second, lead_datetime, min(date_time)) as TAT
    ,min(date_time) as First_call
    ,SUM(total_attempt::int) AS                 no_of_attempts
    ,SUM(call_duration_secs::int) as         total_call_duration
    ,SUM(actual_talk_time_secs::int) as total_talk_time
    ,count(distinct date_time) as                no_of_distinct_tries
    ,avg(actual_talk_time_secs::int) as avg_talk_time
    ,sum(case when actual_talk_time_secs::int > 0 then 1 else 0 end) as total_connected
    FROM 
    (
        SELECT DISTINCT customer_ph_no
        ,date_time
        ,total_attempt
        ,call_duration_secs
        ,actual_talk_time_secs
        FROM dw.c_zentrix_calls
        WHERE date_time is not null
    ) AS B

    JOIN 
    (
        select A.*, case when B.lead_datetime is null then A.lead_date::datetime else B.lead_datetime end as lead_datetime
        from temp3 AS A 
        LEFT join dm_leads As B
        on  A.unbounce_phone  = B.unbounce_phone
        AND A.lead_date = B.lead_date
    ) AS A
    ON  A.unbounce_phone = B.customer_ph_no
    AND B.date_time between A.lead_datetime and A.next_lead_date_call
    group by 1,2,3,4
)


select
distinct sessionid,
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
conc,
source,
campaign_name,
campaign_bucket,
city_abr,
channel_group,
city_group,
sem_bau_bucket,
type,
channel_grouping1,
bau_sub_buckets,
customer_type,
max_cta_id,
qualified_leads,
sch_type,
sch,
sch_date,
txn_type,
txn,
txn_date,
final_amount,
weeknum,
lead_phone,
last_call_date,
last_priority,
last_status,
last_disposition,
last_sub_disposition,
agent_name,
max_disposition,
max_sub_disposition,
max_disposition_sd,
max_sub_disposition_sd,
max_disposition_swk,
max_sub_disposition_swk,
max_disposition_7d,
max_sub_disposition_7d,
max_disposition_14d,
max_sub_disposition_14d,
max_disposition_30d,
max_sub_disposition_30d,
max_disposition_60d,
max_sub_disposition_60d,
rownumb,
no_of_attempts,
total_call_duration,
total_talk_time,
no_of_distinct_tries,
avg_talk_time,
total_connected,
tat,
same_day_flag,
db_flag,
first_call,
lead_datetime,
call_flag
FROM
(        
    select * 
    from dm.dr_affiliates_mid_low_funnel
    where  unbounce_phone <> '-' 
    and unbounce_phone is not null 
    and len(unbounce_phone) = 10 
    and lead_date >= '2020-01-01'
) AS A

LEFT JOIN 
(
    SELECT A.*
    ,B.no_of_attempts
    ,B.total_call_duration
    ,B.total_talk_time
    ,B.no_of_distinct_tries
    ,B.avg_talk_time
    ,B.total_connected
    ,B.TAT
    ,B.same_day_flag
    ,case when C.unbounce_phone is null then 'not in DB' else 'in DB' end as db_flag
    ,B.first_call
    ,B.lead_datetime
    , case when B.unbounce_phone is null then 'not in call data' else 'in call data' end as call_flag
    FROM dispos AS A
    LEFT JOIN call_data as B
    ON A.lead_phone = B.unbounce_phone
    AND A.lead_date2::date = B.lead_date::date
    LEFT JOIN dm_leads as C
    ON A.lead_phone = C.unbounce_phone
    AND A.lead_date2::date = C.lead_date::date
) AS B
ON A.unbounce_phone = B.lead_phone AND A.lead_date::date = B.lead_date2::date
;