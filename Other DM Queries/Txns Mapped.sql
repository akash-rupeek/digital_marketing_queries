TRUNCATE TABLE dm.dr_mapped_txns;
INSERT INTO dm.dr_mapped_txns 
with txns as 

(select
            distinct *,
            coalesce(
                lag(date, 1) over(
                    partition by mobile_num
                    order by
                        date asc
                ),
                '2000-01-01'
            ) AS prev_txn_date,
            datepart(week, date + 1) AS weeknum
        FROM
            (
                select
                    distinct id,
                    mobile_num,
                    date,
                    city,
                    type as txn_type,
                    source,
                    status,
                    a_b_mobile,
                    lead_source,
                    lead_arrival_time,
                    source_priority,
                    txn_amount
                from
                    dm.dr_txns_attributed_to_dm
                
            )
    ),
        
leads AS (
Select DISTINCT unbounce_phone,
lead_date,
source as lead_source_table,
utmcampaign,
campaign_name,
campaign_bucket,
channel_group,
channel_grouping1,
sem_bau_bucket,
type,
bau_sub_buckets,
city_abr,
city_group
from dm.dr_mapped_leads
where LENGTH(unbounce_phone) = 10
),

min_txn_date AS (
        select
            (mobile_num)::varchar as mobile_num,
            min(date) as min_txn_date
        from
            growth.gs_txn_teardown_new
        where
            lower(status) = 'completed'
        group by
            1
    ),

txn_mapped AS 

(
select distinct A.*, B.* 
        , row_number() Over(partition by id,mobile_num,date,prev_txn_date order by coalesce(lead_date, lead_arrival_time) DESC) as row_numb 
        , min(lead_date) over(partition by id,mobile_num,date,prev_txn_date) as first_lead_date

from 
txns AS A
left join leads AS B
on (
        A.mobile_num::varchar = B.unbounce_phone::varchar
        or left(A.a_b_mobile, 10)::varchar = B.unbounce_phone::varchar
        or right(A.a_b_mobile, 10)::varchar = B.unbounce_phone::varchar
        )
        and B.lead_date :: date <= A.date::date

)

select A.*, case
                when C.min_txn_date is null then 'New'
                ELSE 'Repeat'
            END AS customer_type,
            sou.source AS source_mapped
from (
        select distinct id,
        mobile_num,
        date,
        A.city,
        txn_type,
        source,
        status,
        a_b_mobile,
        lead_source,
        lead_arrival_time,
        source_priority,
        txn_amount,
        prev_txn_date,
        unbounce_phone,
        lead_date,
        case when unbounce_phone is null then 'not mapped' else 'mapped' end as DM_data,
        row_numb,
        first_lead_date,
        weeknum,
        lead_source_table,
        utmcampaign,
        CASE WHEN (campaign_bucket is NULL or campaign_bucket in ('', ' ', '#N/A')) THEN 'Not Mapped'
                        ELSE 'Mapped'
                    END AS campaign_mapping_flag,
        CASE
                WHEN (
                    campaign_bucket is NULL
                    or campaign_bucket in ('', ' ', '#N/A')
                ) THEN utmcampaign
                ELSE campaign_name
            END AS campaign_name,
            CASE
                WHEN (
                    campaign_bucket is NULL
                    or campaign_bucket in ('', ' ', '#N/A')
                ) THEN (
                    CASE
                        WHEN lower(lead_source) like '%google%' THEN 'SEM - BAU'
                        WHEN (
                            lower(lead_source) like '%facebook%'
                            or lower(lead_source) like '%fb%'
                        ) THEN 'Facebook - Reach'
                        WHEN lower(lead_source) like '%website%' THEN 'Website'
                    END
                )
                ELSE campaign_bucket
            END AS campaign_bucket,
            CASE
                WHEN (
                    campaign_bucket is NULL
                    or campaign_bucket in ('', ' ', '#N/A')
                ) THEN (
                    CASE
                        WHEN lower(lead_source) like '%google%' THEN 'BAU'
                        WHEN (
                            lower(lead_source) like '%facebook%'
                            or lower(lead_source) like '%fb%'
                        ) THEN 'Facebook - Reach'
                        WHEN lower(lead_source) like '%website%' THEN 'Website'
                    END
                )
                ELSE channel_group
            END AS channel_group,
            CASE
                WHEN (
                    campaign_bucket is NULL
                    or campaign_bucket in ('', ' ', '#N/A')
                ) THEN (
                    CASE
                        WHEN lower(lead_source) like '%google%' THEN 'SEM - BAU'
                        WHEN (
                            lower(lead_source) like '%facebook%'
                            or lower(lead_source) like '%fb%'
                        ) THEN 'Reach Channels'
                        WHEN lower(lead_source) like '%website%' THEN 'Website'
                    END
                )
                ELSE channel_group
            END AS channel_grouping1,
            CASE
                WHEN (
                    campaign_bucket is NULL
                    or campaign_bucket in ('', ' ', '#N/A')
                ) THEN (
                    CASE
                        WHEN lower(lead_source) like '%google%' THEN '0'
                        WHEN (
                            lower(lead_source) like '%facebook%'
                            or lower(lead_source) like '%fb%'
                        ) THEN 'Facebook'
                        WHEN lower(lead_source) like '%website%' THEN 'Website'
                    END
                )
                ELSE sem_bau_bucket
            END AS sem_bau_bucket,
            CASE
                WHEN (
                    campaign_bucket is NULL
                    or campaign_bucket in ('', ' ', '#N/A')
                ) THEN (
                    CASE
                        WHEN lower(lead_source) like '%google%' THEN 'BAU'
                        WHEN (
                            lower(lead_source) like '%facebook%'
                            or lower(lead_source) like '%fb%'
                        ) THEN 'Experiment'
                        WHEN lower(lead_source) like '%website%' THEN 'Website'
                    END
                )
                ELSE type
            END AS type,
            CASE
                WHEN (
                    campaign_bucket is NULL
                    or campaign_bucket in ('', ' ', '#N/A')
                ) THEN (
                    CASE
                        WHEN lower(lead_source) like '%google%' THEN '0'
                        WHEN (
                            lower(lead_source) like '%facebook%'
                            or lower(lead_source) like '%fb%'
                        ) THEN NULL
                        WHEN lower(lead_source) like '%website%' THEN NULL
                    END
                )
                WHEN bau_sub_buckets is null THEN '0'
                ELSE bau_sub_buckets
            END AS bau_sub_buckets,
            CASE
                WHEN (
                    campaign_bucket is NULL
                    or campaign_bucket in ('', ' ', '#N/A')
                    OR city_abr is NULL
                    or city_abr in ('', ' ', '#N/A', '0', 0, 'Other')
                ) THEN coalesce(cm.city_ab, 'Other') ELSE city_abr END city_abr,
        city_group
        from txn_mapped as A
        LEFT JOIN dm.city_mapping as cm
ON lower(A.city) = lower(cm.city)
where row_numb = 1 
) AS A
LEFT JOIN min_txn_date AS C 
        ON A.mobile_num::varchar = C.mobile_num::varchar
            and A.date > C.min_txn_date
LEFT JOIN (
                select
                    distinct id,
                    source
                from
                    (
                        select
                            distinct id,
                            source,
                            row_number() over(
                                partition by id
                                order by
                                    source
                            ) as prio
                        from
                            growth.gs_source_validation
                    )
                where
                    prio = 1
            ) AS sou ON A.id = sou.id
            
;


