with vars as
(
    select 
    '2020-08-01' as start_date
    , current_date - INTEGER '1' AS end_date
    --, '2021-04-04' as end_date
),

leads as
(
    select mobile, city, lead_timestamp, lead_date, utmcampaign, cta_id, mobile_masked
    from
    (
        select 
        unbounce_phone_decrypted as mobile
        , unbounce_phone as mobile_masked
        , lower(city) as city
        , "timestamp" as lead_timestamp
        , "timestamp"::DATE as lead_date
        , utmcampaign
        , cta_id
        , row_number() over (partition by unbounce_phone_decrypted order by "timestamp" asc) as ranking
        from growth.dm_leads
        where "timestamp"::DATE between (select start_date from vars) and (select end_date from vars)
        and lower(utmsource) like '%facebook%'
    ) dm_leads
    where ranking = 1
),

mlf as 
(
    select distinct lead_date, city, mobile, lead_timestamp, utmcampaign, cta_id, schedule_date, txn_date, type, mobile_masked
    from leads

    left join

    (
        select distinct mobile_number_decrypted as schedule_number, max(timestamp::date) AS schedule_date
        from growth.gs_txn_req_all_cities_comb
        where mobile_number_decrypted in (select distinct mobile from leads)
        and date between (select start_date from vars) and (select end_date from vars)
        and lower(status) = 'scheduled'
        group by mobile_number_decrypted
    ) schedule

    on leads.mobile = schedule.schedule_number

    left join

    (
        select mobile_num_decrypted, min(type) as type, max("date"::DATE) as txn_date
        from growth.gs_txn_teardown_new
        where "date" between (select start_date from vars) and (select end_date from vars)
        and mobile_num_decrypted in (select distinct mobile from leads)
        and lower(status) = 'completed'
        group by mobile_num_decrypted
    ) teardown

    on leads.mobile = teardown.mobile_num_decrypted

),

funnel_mapped as
(
    select *
    , datepart(year,lead_date)::int*100+datepart(month,lead_date)::int as month
    , case when txn_date is null then 'not transacted'
    when lead_to_txn_delay < 0 then 'already_converted'
    when lead_to_txn_delay between 0 and 60 then 'txn < 30 days'
    when lead_to_txn_delay > 60 then 'txn > 60 days' end as attribution
    from
    (
        select *
        , DATEPART('day', schedule_date::timestamp - lead_date::timestamp) as lead_to_schedule_delay
        , DATEPART('day', txn_date::timestamp - lead_date::timestamp) as lead_to_txn_delay
        , DATEPART('day', txn_date::timestamp - schedule_date::timestamp) as txn_to_sch_delay
        , DATEPART('day', coalesce(txn_date::timestamp, schedule_date::timestamp) - lead_date::timestamp) as lead_to_conversion_delay
        from mlf
        where txn_date is not null
    ) a
)

select * from funnel_mapped
;