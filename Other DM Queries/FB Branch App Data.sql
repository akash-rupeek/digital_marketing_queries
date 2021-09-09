with vars as
(
    select 
    '2021-06-01' as start_date
    , current_date - INTEGER '1' AS end_date
    --, '2021-04-04' as end_date
    , 45 as app_attribution_window
    , 14 as others_attribution_window
),

top_funnel as 
(
    SELECT campaign_date
    , campaign_name
    , sum(impressions) as impressions   
    , sum(inline_link_clicks) as clicks     
    , sum(spend) as spends  
    from 
    (
        SELECT distinct campaign_date
        , campaign_name
        , impressions   
        , inline_link_clicks    
        , reach 
        , spend 
        , actions   
        , campaign_source   
        , cc_leads  
        , leads_cc  
        , cc_purchase   
        , cc_view_content
        from dm.facebook_add_data
    )a 

    where campaign_name in ('Exp App Campaign Del June', 'Exp App Campaign BLR June', 'Exp App Campaign Mum June', 'Exp App Campaign Sur June')

    group by campaign_date
    , campaign_name
),

branch_data as
(
    select distinct user_data_aaid
    , user_data_developer_identity::varchar as user_data_developer_identity
    , install_activity_timestamp_iso
    , last_attributed_touch_data_dollar_3p, last_attributed_touch_data_tilde_ad_set_name
    , last_attributed_touch_data_tilde_ad_name
    , last_attributed_touch_data_tilde_campaign
    , last_attributed_touch_data_tilde_channel
    , user_data_geo_city_en
    , name
    , timestamp_iso
    from dm.campaign_report_branch
    where last_attributed_touch_data_tilde_campaign in (
        'UAC - Bangalore - Mysuru - June 2021', 'UAC - Delhi - NCR - June 2021'
        , 'Exp App Campaign Del June', 'Exp App Campaign BLR June'
        , 'Exp App Campaign Mum June', 'Exp App Campaign Sur June'
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
        select aaid, devid, channel, campaign
        , listagg(city, ' / ') as cities
        , min(install_timestamp) as install_timestamp
        from
        (
            select distinct user_data_aaid as aaid
            , user_data_developer_identity as devid
            , last_attributed_touch_data_tilde_channel as channel
            , last_attributed_touch_data_tilde_campaign as campaign
            , case when lower(user_data_geo_city_en) like '%delhi%' then 'Delhi' else user_data_geo_city_en end as city
            , install_activity_timestamp_iso as install_timestamp
            from branch_data
            where user_data_aaid is not null
        ) branch_

        group by 1,2,3,4
    ) branch on cuser.id = branch.devid

),


-- Getting leads from app and other campaigns
-- If app lead date < other lead date or (app lead date is not null and other lead date is null) then it's an app paid campaign lead


all_leads as
(
    select phone_number_decrypted as phone, created_at as lead_date, lower(source) as source, lower(utm_source) as utmsource
    from dw.chakra_leads_fresh
    where created_at::date >= (select start_date from vars)
    and phone in (select distinct phone_decrypted from phone_mapped)
),

app_leads as
(
    select * from
    (
        select phone
        , lead_date as last_app_lead_date
        , source as app_source
        , row_number() over (partition by phone order by lead_date desc) as rnk
        from all_leads
        where lower(source) like '%app%'
        and (lower(utmsource) like '%regular%' or lower(utmsource) is null)
    )
    where rnk = 1
),

other_leads as
(
    select phone, source as last_source_channel, lead_date as last_channel_lead_date
    from
    (
        select *
        , row_number() over (partition by phone order by lead_date desc) as rnk
        from all_leads
        where lower(source) not like '%app%' 
    ) others_
    where rnk = 1
),

leads_mapped as
(
    select phone_mapped.*, last_app_lead_date as app_lead_timestamp, last_app_lead_date::date as app_lead_date
    , app_source
    , last_source_channel, last_channel_lead_date
    , case when last_app_lead_date < last_channel_lead_date or (last_channel_lead_date is null and last_app_lead_date is not null) then 1 else 0 end as app_lead
    , COALESCE(last_app_lead_date::date, last_channel_lead_date::date) as lead_date
    from phone_mapped
    left join app_leads on phone_mapped.phone_decrypted = app_leads.phone and phone_mapped.install_timestamp <= last_app_lead_date
    left join other_leads on phone_mapped.phone_decrypted = other_leads.phone
),

push_data_non_ab as 
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

push_data_ab as 
(
    select distinct timestamp::date AS date,
    (LEFT(mobile_number_decrypted::varchar,10))::varchar AS mobile_number,        
    (right(mobile_number_decrypted::varchar,10))::varchar  AS a_b_mobile,
    lower(transaction_type) AS txn_type
    from growth.gs_txn_req_all_cities_comb 
    where  timestamp::date between (select start_date from vars) and (select end_date from vars)
    AND lower(status) = 'scheduled' and len(mobile_number_decrypted) >= 20
),

leads as (select distinct phone_decrypted::varchar as unbounce_phone, lead_date from leads_mapped where lead_date is not null),

sch_data AS 
(
    SELECT customer_phone_decrypted as mobile_number
    , max(date::date) as date
    from growth.rpt_txn_teardown_new 
    where date::date >='2021-01-01'
    GROUP BY 1
),

txn_data AS 
(
    SELECT customer_phone_decrypted as mobile_number
    , date::date as date
    , loan_type AS txn_type
    , SUM(REPLACE(final_amount,',','')::int) as final_amount 
    from growth.rpt_txn_teardown_new 
    where lower(status) = 'completed' 
    and REPLACE(final_amount,',','') != '9923333999' 
    and  final_amount is not null 
    AND date::date >='2021-01-01'
    GROUP BY 1,2,3
),

push_mapped AS 
(
    select distinct unbounce_phone,
    date,
    txn_type,
    lead_date,
    1 AS push
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
            FROM push_data_ab
            JOIN leads
            ON (push_data_ab.mobile_number = leads.unbounce_phone or push_data_ab.a_b_mobile = leads.unbounce_phone)
            AND push_data_ab.date >= leads.lead_date
            group by 1,2,3

            UNION 

            select unbounce_phone,
            date,
            txn_type,
            max(lead_date) as lead_date
            FROM push_data_non_ab
            JOIN leads
            ON push_data_non_ab.mobile_number = leads.unbounce_phone
            AND push_data_non_ab.date >= leads.lead_date
            group by 1,2,3
        )
    ) 
    WHERE row_num = 1
),

sch_mapped AS 
(
    select distinct unbounce_phone,
    date,
    lead_date,
    1 AS sch
    FROM
    (
        select distinct unbounce_phone, date, lead_date
        , row_number() over(partition by unbounce_phone, lead_date order by date asc) as row_num
        from 
        (
            select unbounce_phone,
            date,
            max(lead_date) as lead_date
            FROM sch_data
            JOIN leads
            ON sch_data.mobile_number = leads.unbounce_phone
            AND sch_data.date >= leads.lead_date
            group by 1,2
        )
    ) WHERE row_num = 1 
),

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
            FROM txn_data
            JOIN leads
            ON txn_data.mobile_number = leads.unbounce_phone
            AND txn_data.date >= leads.lead_date
            group by 1,2,3,4
        )
    ) WHERE row_num = 1 
),

ml_funnel AS
(
    select A.*,
    COALESCE(C.txn_type,D.txn_type) as loan_type ,
    case when C.txn = 1 then 1 else COALESCE(B.sch,0) end as sch,
    COALESCE(D.push,0) AS push,
    D.date as push_date,
    B.date AS sch_date,
    COALESCE(C.txn,0) AS txn,
    C.date AS txn_date
    FROM
    (
        select * 
        from leads_mapped
        where phone_decrypted <> '-' 
        and phone_decrypted is not null
    ) AS A

    LEFT JOIN sch_mapped AS B 
    ON A.phone_decrypted::varchar = B.unbounce_phone::varchar ANd A.lead_date::date = B.lead_date::date
    LEFT JOIN txn_mapped AS C
    ON A.phone_decrypted::varchar = C.unbounce_phone::varchar ANd A.lead_date::date = C.lead_date::date
    LEFT JOIN push_mapped AS D
    ON A.phone_decrypted::varchar = D.unbounce_phone::varchar ANd A.lead_date::date = D.lead_date::date
),

lower_funnel as
(
    select channel
    , campaign
    , lead_date
    , count(distinct(aaid)) as total_leads
    , count(distinct(case when app_lead_timestamp is not null then aaid else null end)) as total_app_leads
    , count(distinct(case when app_lead_timestamp is not null and lower(app_source) like '%experiment%' then aaid else null end)) as app_experiment_leads
    , sum(sch) as total_schedules
    , sum(case when app_lead = 1 then sch else 0 end) as schedules_attributed
    , sum(case when app_lead = 1 and lower(loan_type) = 'fresh' then sch else 0 end) as fresh_schedules_attributed
    , sum(txn) as total_txns
    , sum(case when app_lead = 1 then txn else 0 end) as txns_attributed
    , sum(case when app_lead = 1 and lower(loan_type) = 'fresh' then txn else 0 end) as fresh_txns_attributed
    from ml_funnel
    where lead_date is not null
    group by 1,2,3
)

-- select channel
-- , campaign
-- , count(distinct(aaid)) as total_registered
-- , count(distinct(case when app_lead_timestamp is not null then aaid else null end)) as total_app_leads
-- , count(distinct(case when lead_date is not null then aaid else null end)) as total_leads
-- , count(distinct(case when lead_date is null then aaid else null end)) as reg_no_leads
-- , sum(sch) as total_schedules
-- , sum(case when app_lead = 1 then sch else 0 end) as schedules_attributed
-- , sum(case when app_lead = 1 and lower(loan_type) = 'fresh' then sch else 0 end) as fresh_schedules_attributed
-- , sum(txn) as total_txns
-- , sum(case when app_lead = 1 then txn else 0 end) as txns_attributed
-- , sum(case when app_lead = 1 and lower(loan_type) = 'fresh' then txn else 0 end) as fresh_txns_attributed
-- from ml_funnel
-- group by 1,2

-- select channel, campaign
-- , count(case when lead_date BETWEEN '2021-06-20' and '2021-06-26' then phone_decrypted else null end) as leads
-- , count(case when sch = 1 and sch_date BETWEEN '2021-06-20' and '2021-06-26' then phone_decrypted else null end) as schedules
-- , count(case when txn = 1 and txn_date BETWEEN '2021-06-20' and '2021-06-26' then phone_decrypted else null end) as txns
-- from ml_funnel
-- where app_lead = 1

-- select * from ml_funnel

-- select tf.*, lf.*
-- from top_funnel tf
-- right join lower_funnel lf
-- on tf.campaign_name = lf.campaign and tf.campaign_date = lf.lead_date


select ml_funnel.*, calls_made, calls_connected
from ml_funnel

left join

(
    select distinct phone_decrypted, count(call_time) as calls_made
    , count(case when customer_talk_time > 0 then call_time else null end) as calls_connected
    from dw.ameyo_call_details b
    left join  dw.chakra_leads_fresh a on a.phone_number = phone and call_time >=  created_at
    where lower(source) like ('%app%')
    and (lower(utm_source) like ('%regular%') or lower(utm_source) is null)
    and created_at::date between '2021-06-01' and getdate()::date
    and json_extract_path_text(data, 'duplicate',TRUE) = ''
    group by 1
) calls

on ml_funnel.phone_decrypted = calls.phone_decrypted
;