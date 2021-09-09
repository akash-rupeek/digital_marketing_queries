DELETE FROM  dm.dr_txns_attributed_to_dm where datepart(year,date::date)::int*100+ datepart(month,date::date)::int 
                >= (select  case when datepart('month',getdate()) =  1 then  (datepart('year',getdate())::int - 1) * 100 + 12  else datepart('year',getdate())::int * 100 + datepart('month',getdate())::int-1 end);
INSERT INTO dm.dr_txns_attributed_to_dm 
select * from
(
with txns_mapped_source AS
(
        select distinct
        teardown.id,
        teardown.mobile_num::varchar as mobile_num,
        teardown.date,
        teardown.city,
        teardown.type,
        coalesce(sourcetracker.source,'missing in source tracker') as source,
        teardown.status,
        teardown.a_b_customer,
        teardown.asgn_3_name,
        replace(teardown.a_b_mobile,',','')::varchar AS a_b_mobile ,
        sum(replace(teardown.final_amount,',','')::float) as txn_amount
        from growth.gs_txn_teardown_new AS teardown
        left join (select distinct id, source 
                                from (
                                        select distinct id, source , row_number() over(partition by id order by source) as prio 
                                        from growth.gs_source_validation
                                        ) 
                                where prio = 1) AS sourcetracker
        ON teardown.id = sourcetracker.id
        where teardown.date::date between '2020-01-01' and getdate()-1
        and datepart(year,teardown.date::date)::int*100+ datepart(month,teardown.date::date)::int 
                >= (select  case when datepart('month',getdate()) =  1 then  (datepart('year',getdate())::int - 1) * 100 + 12  else datepart('year',getdate())::int * 100 + datepart('month',getdate())::int-1 end)
        and lower(teardown.status)='completed'
        group by 1,2,3,4,5,6,7,8,9,10
),

final_leads AS 

(

select *,  CASE
        when lower(source) like '%goog%' then '1'
        when lower(source) ='google' then '1'
        when lower(source) like '%fb%' then '1'
        when lower(source) LIKE '%acebook%' then '1'
        when lower(source) like '%web%' then '1'
        when lower(source) like '%you%' then '1'
        when lower(source) like '%generic%' then '1'
        when lower(source) like '%brand%' then '1'
        else '2'
    end as source_priority
FROM(
(select
    phone_growth::varchar as lead_phone,
    lead_arrival_time::TIMESTAMP,
    source::TEXT
from
    growth.stg_all_leads_funnel)
UNION
(with leads as( SELECT
                distinct phone_customer::varchar as phone_customer,
                currentcity_customer,
                lead_date :: TIMESTAMP,
                vendor,
                score,
                medium
            FROM
                growth.all_affiliate_data
            union
            SELECT
                DISTINCT phone_customer::varchar as phone_customer,
                currentcity_customer,
                lead_date :: TIMESTAMP,
                'm1_&_m2' vendor,
                null as score,
                null as medium
            from
                growth.m_one_m_two
            union
            SELECT
                DISTINCT phone::varchar as phone_customer,
                city,
                date :: date,
                'indialends' vendor,
                quality as score,
                null as medium
            FROM
                growth.gs_indialends_leads
        )
        Select
            distinct phone_customer,
            lead_date :: TIMESTAMP as first_lead_date,
            vendor
        from
            leads
)
UNION
(SELECT
        DISTINCT mobile_num::varchar as mobile_num,
        date :: date,
        channel
    FROM
        sales.gs_sales_trx_completed_tmp
        where lower(status)='completed'
        
        and LOWER(channel)<>'company lead')
UNION
(SELECT 
    mobile_number::varchar as mob_num,
    timestamp::TIMESTAMP as lead_arrival,
    final_team::TEXT
FROM
    sales.gs_sales_all_data_dump_source
    where lower(final_team) not in ('company lead')
))),

txns_mapped_leads AS (
select * from
(
        select id,mobile_num,date,
        city,
        type,
        a.source,
        status,
        a_b_customer,
        asgn_3_name,
        a_b_mobile,
        txn_amount,
        case when final_leads.source is null and lower(a.source)='ops' then 'OPS' else final_leads.source end as lead_source,
        final_leads.lead_arrival_time,coalesce(final_leads.source_priority,'2') AS source_priority,
        row_number() over (partition by id order by coalesce(final_leads.source_priority,'2'),(a.date-final_leads.lead_arrival_time) desc) as row_index
        FROM (select * from txns_mapped_source where a_b_mobile is null) a
        left join  (select distinct * from final_leads where lead_phone is not null and lead_phone <>'' and len(lead_phone) >= 10) as final_leads
        on a.mobile_num::varchar=final_leads.lead_phone::varchar
        and a.date::Date>=final_leads.lead_arrival_time::date
        and (a.date::Date-interval'60 days')::date<=final_leads.lead_arrival_time::date
        order by 1,13
        )
        where row_index = 1
UNION
        select * from
        (
        select id,mobile_num,date,
        city,
        type,
        a.source,
        status,
        a_b_customer,
        asgn_3_name,
        a_b_mobile,
        txn_amount,
        case when final_leads.source is null and lower(a.source)='ops' then 'OPS' else final_leads.source end as lead_source,
        final_leads.lead_arrival_time,coalesce(final_leads.source_priority,'2') AS source_priority,
        row_number() over (partition by id order by coalesce(final_leads.source_priority,'2'),(a.date-final_leads.lead_arrival_time) desc) as row_index
        FROM (select * from txns_mapped_source where a_b_mobile is not null) a
        left join  (select distinct * from final_leads where lead_phone is not null and lead_phone <>'' and len(lead_phone) >= 10) as final_leads
        on (a.mobile_num::varchar=final_leads.lead_phone::varchar
                        or left(a.a_b_mobile,10)::varchar = final_leads.lead_phone::varchar 
                        or right(a.a_b_mobile,10)::varchar = final_leads.lead_phone::varchar)
        and a.date::Date>=final_leads.lead_arrival_time::date
        and (a.date::Date-interval'60 days')::date<=final_leads.lead_arrival_time::date
        order by 1,13
        )
        where row_index = 1
),

other_txns as (

        with null_txns as 
        (
        select distinct *
        from txns_mapped_leads
        where source_priority!=1
        and row_index=1
        and lead_source is null
        )


select id,
mobile_num,
date,
city,
type,
null_txns.source,
status,
a_b_customer,
asgn_3_name,
a_b_mobile,
txn_amount,
final_leads.source as lead_source,
final_leads.lead_arrival_time,coalesce(final_leads.source_priority,'2') AS source_priority,
row_number() over (partition by id order by coalesce(final_leads.source_priority,'2'),(null_txns.date-final_leads.lead_arrival_time) desc) as row_index
from (select * from null_txns where a_b_mobile is null) null_txns
left join  (select distinct * from final_leads where lead_phone is not null and lead_phone <>'' and len(lead_phone) >= 10) as final_leads
on null_txns.mobile_num::varchar =final_leads.lead_phone::varchar
and null_txns.date::Date>=final_leads.lead_arrival_time::date

UNION

select id,
mobile_num,
date,
city,
type,
null_txns.source,
status,
a_b_customer,
asgn_3_name,
a_b_mobile,
txn_amount,
final_leads.source as lead_source,
final_leads.lead_arrival_time,coalesce(final_leads.source_priority,'2') AS source_priority,
row_number() over (partition by id order by coalesce(final_leads.source_priority,'2'),(null_txns.date-final_leads.lead_arrival_time) desc) as row_index
from (select * from null_txns where a_b_mobile is not null) null_txns
left join  (select distinct * from final_leads where lead_phone is not null and lead_phone <>'' and len(lead_phone) >= 10) as final_leads
on (null_txns.mobile_num::varchar =final_leads.lead_phone::varchar
                        or left(null_txns.a_b_mobile,10)::varchar = final_leads.lead_phone::varchar 
                        or right(null_txns.a_b_mobile,10)::varchar = final_leads.lead_phone::varchar)
and null_txns.date::Date>=final_leads.lead_arrival_time::date

)



(
select distinct *
from txns_mapped_leads
where source_priority=1
and row_index=1)

UNION 

(
select distinct *
from txns_mapped_leads
where source_priority!=1
and row_index=1
and lead_source is not null)

UNION

(

select distinct *
from other_txns
where source_priority=1
and row_index=1)

UNION

(
select distinct *
from other_txns
where source_priority!=1
and row_index=1 ) 

)
where source_priority = 1 or lead_source in ('yoads',
'facebook-paid',
'intellactads',
'Facebook',
'google',
'affipedia',
'website',
'FB',
'facebook')

;
