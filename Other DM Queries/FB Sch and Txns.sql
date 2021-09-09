with vars as
(
    select 
    '2020-10-01' as start_date
    --, current_date - INTEGER '1' AS end_date
    , '2021-03-31' as end_date
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
        , lead_timestamp
        , lead_date
        , utmcampaign
        , max_cta_id as cta_id
        , row_number() over (partition by unbounce_phone_decrypted order by lead_timestamp desc) as ranking
        from dm.dr_mapped_leads
        where lead_date between (select start_date from vars) and (select end_date from vars)
        and lower(sem_bau_bucket) = 'facebook'
    ) dm_leads
    where ranking = 1
),

mlf as 
(
    select distinct lead_date, city, mobile, cta_id, schedule_date, txn_date, mobile_masked, coalesce(txn_type, sch_type) as lead_type
    from leads

    left join

    (
        select distinct mobile_num as schedule_number, max(date) AS schedule_date, max(lower(txn_type)) as sch_type
        from dm.dr_mapped_schs
        where mobile_num in (select distinct mobile_masked from leads)
        and "date" between (select start_date from vars) and (select end_date from vars)
        and lower(status) = 'scheduled'
        group by mobile_num
    ) schedule

    on leads.mobile_masked = schedule.schedule_number

    left join

    (
        select distinct mobile_num as txn_number, max(date) AS txn_date, max(lower(txn_type)) as txn_type
        from dm.dr_mapped_txns
        where mobile_num in (select distinct mobile_masked from leads)
        and "date" between (select start_date from vars) and (select end_date from vars)
        and lower(status) = 'completed'
        group by mobile_num
    ) txn

    on leads.mobile_masked = txn.txn_number

)

select *
, case when schedule_date is not null then 1 else 0 end as sch
, case when txn_date is not null then 1 else 0 end as txn
, datepart(year,lead_date)::int*100+datepart(month,lead_date)::int as lead_month
, datepart(year,schedule_date)::int*100+datepart(month,schedule_date)::int as sch_month
, datepart(year,txn_date)::int*100+datepart(month,txn_date)::int as txn_month
from mlf
;


with vars as
(
    select 
    '2020-10-01' as start_date
    --, current_date - INTEGER '1' AS end_date
    , '2021-03-31' as end_date
),

sch as
(
    select distinct mobile_num as schedule_number, max(date) AS schedule_date, max(lower(txn_type)) as sch_type
    from dm.dr_mapped_schs
    where "date" between (select start_date from vars) and (select end_date from vars)
    and lower(status) = 'scheduled'
    group by mobile_num
), 

txn as
(
    select distinct mobile_num as txn_number, max(date) AS txn_date, max(lower(txn_type)) as txn_type
    from dm.dr_mapped_txns
    where "date" between (select start_date from vars) and (select end_date from vars)
    and lower(status) = 'completed'
    group by mobile_num
)

select distinct coalesce(sch.schedule_number,txn.txn_number) as mobile , coalesce(txn_type, sch_type) as lead_type
from sch
full outer join txn
on schedule_number = txn_number
;