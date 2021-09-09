with vars as
(
    select '2021-04-01' as start_date
    , '2021-04-30' as end_date
),

leads_data as
(
    select
    case when lower(TRIM(source)) like '%google%' then 'brand' when lower(TRIM(source)) like '%website%' then 'website' else lower(TRIM(source)) end as channel
    , datepart(year,lead_arrival_time)::int*100+datepart(month,lead_arrival_time)::int as mnth
    , lead_arrival_time::date as lead_date
    , lower(TRIM(city)) as city
    , count(distinct(lead_phone)) as leads
    from growth.stg_all_leads_funnel
    where lower(customer_type) = 'new'
    and (lower(TRIM(source)) in ('app', 'organic', 'website') or (lower(TRIM(source)) like '%google%' and lower(campaign_growth) = 'brand') or lower(TRIM(source)) like '%website%')
    and lead_arrival_time::date between (select start_date from vars) and (select end_date from vars)
    and city is not null
    group by 1,2,3,4
),

txn_data as
(
    select lower(TRIM(city)) as city, lead_date, lower(TRIM(channel)) as channel
    , datepart(year,lead_date)::int*100+datepart(month,lead_date)::int as mnth
    , count(distinct(case when lower(status) = 'completed' then mobile_num else null end)) as txns
    , count(distinct(case when lower(status) = 'completed' and lower(loan_type) = 'fresh' then mobile_num else null end)) as fresh_txns

    from
    (
        select * from
        (
            select distinct mobile_num, city, loan_type, status, del.lead_date, cust_type, bucket as channel
            from
            (
                select distinct lower(trim(city)) as city, "type" as loan_type, status, lead_arrival_time::date as lead_date, cust_type, mobile_num
                FROM growth.rpt_delivery_dashboard_v1
                where lower(lead_source) like '%google%'
                and lead_arrival_time::date between (select start_date from vars) and (select end_date from vars)
            ) del
            
            left join
            
            (
                select distinct unbounce_phone, lead_date, lower(sem_bau_bucket) as bucket
                from dm.dr_mapped_leads
                where lower(utmsource) like '%google%'
                and lead_date between (select start_date from vars) and (select end_date from vars)
            ) dmleads
            
            on del.mobile_num = dmleads.unbounce_phone and del.lead_date = dmleads.lead_date
            
            where lower(bucket) = 'brand'
        ) brandLeads

        union all
        
        select *
        from
        (
            select distinct mobile_num, lower(trim(city)) as city, "type" as loan_type, status, lead_arrival_time::date as lead_date, cust_type
            , case when lower(lead_source) like '%website%' then 'website' else lower(trim(lead_source)) end as channel
            from growth.rpt_delivery_dashboard_v1
            where (lower(trim(lead_source)) in ('app', 'organic') or lower(lead_source) like '%website%')
            and lead_arrival_time::date between (select start_date from vars) and (select end_date from vars)
        ) awo

    ) leadsCombined
    where lower(cust_type) = 'new'
    group by 1,2,3,4
)

select leads_data.*, txn_data.txns, txn_data.fresh_txns
from leads_data
left join txn_data
on leads_data.channel = txn_data.channel and leads_data.city = txn_data.city and leads_data.lead_date = txn_data.lead_date
;