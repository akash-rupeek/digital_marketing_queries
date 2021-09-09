TRUNCATE TABLE dm.dr_affiliates_mid_low_funnel_v2;

INSERT INTO dm.dr_affiliates_mid_low_funnel_v2

-- CREATE TABLE dm.dr_affiliates_mid_low_funnel_v2 AS

with sch_data_non_ab as 
(
select distinct timestamp::date AS date,
        (case when len(mobile_number) < 14 then right(mobile_number::varchar,10)  
                when len(mobile_number) >= 14 then LEFT(mobile_number::varchar,10)  END)::varchar
        AS mobile_number,        
        lower(transaction_type) AS txn_type
from growth.gs_txn_req_all_cities_comb 
where  timestamp::date between '2020-01-01'  and getdate()-1
AND lower(status) = 'scheduled' and len(mobile_number) >9 and len(mobile_number) < 20
),

sch_data_ab as 
(
select distinct timestamp::date AS date,
        (LEFT(mobile_number::varchar,10))::varchar AS mobile_number,        
        (right(mobile_number::varchar,10))::varchar  AS a_b_mobile,
        lower(transaction_type) AS txn_type
from growth.gs_txn_req_all_cities_comb 
where  timestamp::date between '2020-01-01'  and getdate()-1
AND lower(status) = 'scheduled' and len(mobile_number) >= 20
),



leads as 
(select distinct unbounce_phone::varchar as unbounce_phone, lead_date from dm.dr_affiliates_mapped_leads_v2 where lead_date >= '2020-01-01'),


txn_data_ab AS 
(
SELECT (replace(mobile_num,',',''))::varchar AS mobile_number
        , (replace(a_b_mobile,',',''))::varchar AS a_b_mobile
        , date::date as date
        , date::date as created_at
        , SUM(REPLACE(txn_amount,',','')::int) as final_amount 
        , type AS txn_type
from growth.rpt_delivery_dashboard_v1
where lower(status) = 'completed' 
        and REPLACE(txn_amount,',','') != '9923333999' 
        and  final_amount is not null 
        AND date::date >='2020-01-01'
        AND a_b_mobile is not null 
GROUP BY 1,2,3,4,6
),

txn_data_non_ab as 
(SELECT (replace(mobile_num,',',''))::varchar AS mobile_number
        , (replace(a_b_mobile,',',''))::varchar AS a_b_mobile
        , date::date as date
        , date::date as created_at
        , SUM(REPLACE(txn_amount,',','')::int) as final_amount 
        , type AS txn_type
from growth.rpt_delivery_dashboard_v1
where lower(status) = 'completed' 
        and REPLACE(txn_amount,',','') != '9923333999' 
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
) WHERE row_num = 1
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
)


select A.*,
        B.txn_type as sch_type ,
        COALESCE(B.sch,0) AS sch,
        COALESCE(B.date,'2000-01-01') AS sch_date,
        C.txn_type as txn_type ,
        COALESCE(C.txn,0) AS txn,
        COALESCE(C.date,'2000-01-01')  AS txn_date,
        final_amount,
        datepart('week', A.lead_date) AS weeknum
FROM
(        select * 
        from dm.dr_affiliates_mapped_leads_v2
        where  unbounce_phone <> '-' 
                and unbounce_phone is not null 
                and len(unbounce_phone) >= 10 
                and lead_date >= '2020-01-01'
) AS A

LEFT JOIN sch_mapped AS B 
ON A.unbounce_phone = B.unbounce_phone 
ANd A.lead_date = B.lead_date
LEFT JOIN txn_mapped AS C
ON A.unbounce_phone = C.unbounce_phone 
ANd A.lead_date = C.lead_date
;