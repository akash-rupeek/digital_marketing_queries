DELETE from dm.dr_schs_attributed_to_dm where datepart(year,date::date)::int*100+ datepart(month,date::date)::int 
                >= (select  case when datepart('month',getdate()) =  1 then  (datepart('year',getdate())::int - 1) * 100 + 12  else datepart('year',getdate())::int * 100 + datepart('month',getdate())::int-1 end);
                
Insert into  dm.dr_schs_attributed_to_dm 
select * from
(
with sch_data AS
(
        select distinct timestamp::date AS date,
        city,
        lower(status) AS status,
        (case when len(mobile_number) < 20 THEN right(mobile_number,10) ELSE LEFT(mobile_number,10) END)::varchar AS mobile_num,        
        lower(transaction_type) AS type,
        source,
        case when len(mobile_number) > 19 THEN right(mobile_number,10)::varchar END AS a_b_mobile,
        row_number() OVER() AS id
        from growth.gs_txn_req_all_cities_comb 
        where  timestamp::date between '2020-01-01'  and getdate()-1 and
        datepart(year,timestamp::date)::int*100+ datepart(month,timestamp::date)::int 
        >= (select  case when datepart('month',getdate())::int =  1 then  (datepart('year',getdate())::int - 1) * 100 + 12  else datepart('year',getdate())::int * 100 + datepart('month',getdate())::int-1 end)
        AND lower(status) = 'scheduled' and len(mobile_number) between 9 and 19
),

sch_data2 AS
(
        select distinct timestamp::date AS date,
        city,
        lower(status) AS status,
        (case when len(mobile_number) < 20 THEN right(mobile_number,10) ELSE LEFT(mobile_number,10) END)::varchar AS mobile_num,        
        lower(transaction_type) AS type,
        source,
        case when len(mobile_number) > 19 THEN right(mobile_number,10)::varchar END AS a_b_mobile,
        row_number() OVER() AS id
        from growth.gs_txn_req_all_cities_comb 
        where  timestamp::date between '2020-01-01'  and getdate()-1 and
        datepart(year,timestamp::date)::int*100+ datepart(month,timestamp::date)::int 
        >= (select  case when datepart('month',getdate()) =  1 then  (datepart('year',getdate())::int - 1) * 100 + 12  else datepart('year',getdate())::int * 100 + datepart('month',getdate())::int-1 end)
        AND lower(status) = 'scheduled' and len(mobile_number) > 19
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
    FROM
    (
        (   
            select
            phone_growth::varchar as lead_phone,
            lead_arrival_time::TIMESTAMP,
            source::TEXT
            from
            growth.stg_all_leads_funnel
        )
    UNION
(
    with leads as
    (
        SELECT
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
    distinct phone_customer::varchar AS phone_customer,
    lead_date :: TIMESTAMP as first_lead_date,
    vendor
    from
    leads
)

UNION
(SELECT
        DISTINCT mobile_num::varchar AS mobile_num,
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


schs_mapped_leads AS (
select * from
(
select 
id,
mobile_num::VARCHAR AS mobile_num,
date,
city,
type,
a.source,
status,
a_b_mobile::VARCHAR AS a_b_mobile,
case when final_leads.source is null and lower(a.source)='ops' then 'OPS' else final_leads.source end as lead_source,
final_leads.lead_arrival_time,coalesce(final_leads.source_priority,'2') AS source_priority,
row_number() over (partition by id order by coalesce(final_leads.source_priority,'2'),(a.date-final_leads.lead_arrival_time) desc) as row_index
FROM
sch_data a
left join  final_leads
on a.mobile_num::varchar=final_leads.lead_phone::varchar 
and a.date::Date>=final_leads.lead_arrival_time::date
and (a.date::Date-interval'60 days')::date<=final_leads.lead_arrival_time::date

)
where row_index = 1
UNION 

select * from
(
select 
id,
mobile_num::VARCHAR AS mobile_num,
date,
city,
type,
a.source,
status,
a_b_mobile::VARCHAR AS a_b_mobile,
case when final_leads.source is null and lower(a.source)='ops' then 'OPS' else final_leads.source end as lead_source,
final_leads.lead_arrival_time,coalesce(final_leads.source_priority,'2') AS source_priority,
row_number() over (partition by id order by coalesce(final_leads.source_priority,'2'),(a.date-final_leads.lead_arrival_time) desc) as row_index
FROM
sch_data2 a
left join  final_leads
on (a.mobile_num::varchar=final_leads.lead_phone::varchar  or a.a_b_mobile::varchar = final_leads.lead_phone::varchar)
and a.date::Date>=final_leads.lead_arrival_time::date
and (a.date::Date-interval'60 days')::date<=final_leads.lead_arrival_time::date

)
where row_index = 1
)



(
select distinct 
mobile_num,
date,
city,
type,
source,
status,
a_b_mobile,
lead_arrival_time,
lead_source,
source_priority,
row_index
from schs_mapped_leads
where source_priority=1
and row_index=1)

UNION 

(
select distinct 
mobile_num,
date,
city,
type,
source,
status,
a_b_mobile,
lead_arrival_time,
lead_source,
source_priority,
row_index
from schs_mapped_leads
where source_priority!=1
and row_index=1
and lead_source is not null)

UNION

(with other_schs as (with null_schs as (
select distinct 
id,
mobile_num,
date,
city,
type,
source,
status,
a_b_mobile,
lead_source,
source_priority,
row_index
from schs_mapped_leads
where source_priority!=1
and row_index=1
and lead_source is null)


select 
id,
mobile_num::varchar AS mobile_num,
date,
city,
type,
null_schs.source,
status,
a_b_mobile::varchar AS a_b_mobile,
final_leads.source as lead_source,
final_leads.lead_arrival_time,coalesce(final_leads.source_priority,'2') AS source_priority,
row_number() over (partition by id order by coalesce(final_leads.source_priority,'2'),(null_schs.date-final_leads.lead_arrival_time) desc) as row_index
from null_schs
left join  final_leads
on null_schs.mobile_num::varchar = final_leads.lead_phone::varchar
and null_schs.date::Date>=final_leads.lead_arrival_time::date
)

select distinct 
mobile_num,
date,
city,
type,
source,
status,
a_b_mobile,
lead_arrival_time,
lead_source,
source_priority,
row_index
from other_schs
where source_priority=1
and row_index=1)

UNION

(with other_schs as (with null_schs as (
select distinct 
id,
mobile_num,
date,
city,
type,
source,
status,
a_b_mobile,
lead_source,
source_priority,
row_index
from schs_mapped_leads
where source_priority!=1
and row_index=1
and lead_source is null)


select 
id,
mobile_num::varchar AS mobile_num,
date,
city,
type,
null_schs.source,
status,


a_b_mobile::varchar AS a_b_mobile,
final_leads.source as lead_source,
final_leads.lead_arrival_time,coalesce(final_leads.source_priority,'2') AS source_priority,
row_number() over (partition by id order by coalesce(final_leads.source_priority,'2'),(null_schs.date-final_leads.lead_arrival_time) desc) as row_index
from null_schs
left join  final_leads
on null_schs.mobile_num::varchar =final_leads.lead_phone::varchar
and null_schs.date::Date>=final_leads.lead_arrival_time::date
)

select distinct 
mobile_num,
date,
city,
type,
source,
status,
a_b_mobile,
lead_arrival_time,
lead_source,
source_priority,
row_index
from other_schs
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

