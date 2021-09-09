with vars as
(
    select 
    '2020-12-01' as start_date
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
    from temp.ctc_completion
),

exp_list as
(
    select city, campaign_type, campaign_name, did_number, campaign_id
    from temp.ctc_experiments_list
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

inbound as
(
    select cz.*, campaign_name, 'Test'::text as campaign_type, city
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
),

dm_leads as 
(
    select distinct dml.*, campaign_type
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

    ) dml
    left join exp_list
    on dml.utmcampaign = cast(exp_list.campaign_id as VARCHAR)
),

mldump as
(
    select mlf.*, campaign_type
    from
    (
        select lead_date, sch, sch_date, txn, txn_date
        , weeknum, right(unbounce_phone_decrypted,10) as unbounce_phone_decrypted, utmsource, utmcampaign, campaign_name, city_abr, city
        from dm.dr_mid_low_funnel_old_attr
        where utmcampaign in (select distinct cast(campaign_id as varchar) from exp_list)
        and lead_date between (select start_date from vars) and (select end_date from vars)
    ) mlf
    left join exp_list
    on mlf.utmcampaign = cast(exp_list.campaign_id as VARCHAR)
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

leads as 
(
    select lead_date, city, campaign_type
    , count(distinct(case when call_type = 'inbound' then mobile else null end)) as inbound_leads
    , count(distinct(case when call_type = 'outbound' then mobile else null end)) as outbound_leads
    , count(distinct(mobile)) as total_leads
    from
    (
        select distinct lead_date, lower(city) as city, campaign_type, mobile, 'inbound'::text call_type
        from inbound

        union all

        select distinct date1, city, campaign_type, unbounce_phone_decrypted, 'outbound'::text as call_type
        from dm_leads
    ) leads_combined
    group by lead_date, city, campaign_type
),

all_leads as
(
    select distinct lead_date, city, campaign_type, mobile, call_type
    from
    (
        select distinct lead_date, lower(city) as city, campaign_type, mobile, 'inbound'::text call_type
        from inbound

        union all

        select distinct date1, city, campaign_type, unbounce_phone_decrypted, 'outbound'::text as call_type
        from dm_leads
    ) inb_dm
),

mlfunnel as
(
    select lead_date, city, campaign_type
    , count(distinct(unbounce_phone_decrypted)) as leads_attributed
    , sum(sch) as schedules
    , sum(txn) as transactions
    from mldump
    group by lead_date, city, campaign_type
),

top_mid_funnel as
(
    select campaign_date, top_ctc.city, top_ctc.campaign_type
    , impressions, clicks, spends, ctc_completions
    , inbound_leads, outbound_leads, total_leads
    from top_ctc
    left join leads
    on top_ctc.campaign_date = leads.lead_date
    and lower(top_ctc.city) = lower(leads.city)
    and lower(top_ctc.campaign_type) = lower(leads.campaign_type) 
),

aib_txn as 
(
    select lead_date, city, campaign_type
    , count(distinct(mobile_num_decrypted)) as aib_txn
    from
    (
        select right(mobile_num_decrypted,10) as mobile_num_decrypted, max("date"::DATE) as txn_date
        from growth.gs_txn_teardown_new
        where "date" between (select start_date from vars) and (select end_date from vars)
        and mobile_num_decrypted in (select distinct mobile from inbound)
        and lower(status) = 'completed'
        group by mobile_num_decrypted
    ) teardown
    
    left join inbound

    on teardown.mobile_num_decrypted = inbound.mobile and teardown.txn_date >= inbound.lead_date

    group by lead_date, city, campaign_type

),

all_sch as 
(
    select lead_date, city, campaign_type, sum(cast(schedule as int)) as total_schedules
    from
    (
        select distinct lead_date, city, campaign_type, mobile, call_type
        , case when schedule_date is not null and schedule_date >= lead_date then 1 else 0 end as schedule
        from all_leads

        left join

        (
            select * from
            (
                select distinct right(mobile_number_decrypted,10) as schedule_number, timestamp::date AS schedule_date
                , rank() over (partition by mobile_number order by "timestamp" desc) as rnk
                from growth.gs_txn_req_all_cities_comb
                where mobile_number_decrypted in (select distinct mobile from all_leads)
                and date between (select start_date from vars) and (select end_date from vars)
                and lower(status) = 'scheduled'
            ) xx
            where rnk = 1
        ) schedule

        on all_leads.mobile = schedule.schedule_number
    ) schedule_mapped
    group by lead_date, city, campaign_type

),

ibfl_sch as 
(
    select lead_date, city, campaign_type, sum(cast(schedule as int)) as ibfl_schedules
    from
    (
        select distinct lead_date, city, campaign_type, mobile
        , case when schedule_date is not null and schedule_date >= lead_date then 1 else 0 end as schedule
        from inbound

        left join

        (
            select * from
            (
                select distinct right(mobile_number_decrypted,10) as schedule_number, timestamp::date AS schedule_date
                , rank() over (partition by mobile_number order by "timestamp" desc) as rnk
                from growth.gs_txn_req_all_cities_comb
                where mobile_number_decrypted in (select distinct mobile from all_leads)
                and date between (select start_date from vars) and (select end_date from vars)
                and lower(status) = 'scheduled'
            ) xx
            where rnk = 1
        ) schedule

        on inbound.mobile = schedule.schedule_number
    ) schedule_mapped
    group by lead_date, city, campaign_type

),

all_funnel as
(
    select tmf.*, leads_attributed, schedules, transactions, aib_txn, total_schedules, ibfl_schedules
    from top_mid_funnel tmf
    left join mlfunnel mlf
    on tmf.campaign_date = mlf.lead_date
    and lower(tmf.city) = lower(mlf.city)
    and tmf.campaign_type = mlf.campaign_type

    left join aib_txn
    on tmf.campaign_date = aib_txn.lead_date
    and lower(tmf.city) = lower(aib_txn.city)
    and tmf.campaign_type = aib_txn.campaign_type

    left join all_sch
    on tmf.campaign_date = all_sch.lead_date
    and lower(tmf.city) = lower(all_sch.city)
    and tmf.campaign_type = all_sch.campaign_type

    left join ibfl_sch
    on tmf.campaign_date = ibfl_sch.lead_date
    and lower(tmf.city) = lower(ibfl_sch.city)
    and tmf.campaign_type = ibfl_sch.campaign_type
)

select *
, case when lower(city) in ('ahmedabad', 'jaipur') then 'Call_Primary'
when lower(city) in ('surat', 'noida', 'faridabad') then 'Apply_Primary' end as city_group
, COALESCE(transactions,0) + COALESCE(aib_txn,0) as total_txn
from all_funnel
;