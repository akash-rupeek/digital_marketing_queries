with vars as
(
    select 
    '2021-06-01' as start_date
    -- '2021-04-01' as start_date
    , current_date - INTEGER '1' AS end_date
    --, '2021-04-04' as end_date
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
	where last_attributed_touch_data_tilde_ad_set_name in ('Exp_App_BLR_June', 'Exp_App_Del_June')
),

phone_mapped as
(
	select id, leadid, phone_decrypted, phones_decrypted, max(created_at::date) as lead_date
	from
	(
	    select distinct id, leadid, phone_decrypted, phones_decrypted, created_at
	    from dw.core_user
	    where id in (select distinct user_data_developer_identity from branch_data where user_data_developer_identity is not null)
	    
	    union all
	    select distinct id, leadid, phone_decrypted, phones_decrypted, created_at
	    from dw.core_user
	    where leadid in (select distinct user_data_developer_identity from branch_data where user_data_developer_identity is not null)
	) devIDmap
	group by 1,2,3,4
),

sch_data_non_ab as 
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

sch_data_ab as 
(
    select distinct timestamp::date AS date,
    (LEFT(mobile_number_decrypted::varchar,10))::varchar AS mobile_number,        
    (right(mobile_number_decrypted::varchar,10))::varchar  AS a_b_mobile,
    lower(transaction_type) AS txn_type
    from growth.gs_txn_req_all_cities_comb 
    where  timestamp::date between (select start_date from vars) and (select end_date from vars)
    AND lower(status) = 'scheduled' and len(mobile_number_decrypted) >= 20
),

leads as (select distinct phone_decrypted::varchar as unbounce_phone, lead_date from phone_mapped),

txn_data_ab AS 
(
    SELECT (replace(mobile_num_decrypted,',',''))::varchar AS mobile_number
    , (replace(a_b_mobile_decrypted,',',''))::varchar AS a_b_mobile
    , date::date as date
    , date::date as created_at
    , SUM(REPLACE(final_amount,',','')::int) as final_amount 
    , type AS txn_type
    from growth.gs_txn_teardown_new 
    where lower(status) = 'completed' 
    and REPLACE(final_amount,',','') != '9923333999' 
    and  final_amount is not null 
    AND date::date >='2020-01-01'
    AND a_b_mobile is not null 
    GROUP BY 1,2,3,4,6
),

txn_data_non_ab as 
(
    SELECT (replace(mobile_num_decrypted,',',''))::varchar AS mobile_number
    , (replace(a_b_mobile_decrypted,',',''))::varchar AS a_b_mobile
    , date::date as date
    , date::date as created_at
    , SUM(REPLACE(final_amount,',','')::int) as final_amount 
    , type AS txn_type
    from growth.gs_txn_teardown_new 
    where lower(status) = 'completed' 
    and REPLACE(final_amount,',','') != '9923333999' 
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
    ) 
    WHERE row_num = 1
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
),

ml_funnel AS
(
    select A.*,
    B.txn_type as sch_type ,
    COALESCE(B.sch,0) AS sch,
    B.date AS sch_date,
    C.txn_type as txn_type ,
    COALESCE(C.txn,0) AS txn,
    C.date AS txn_date
    FROM
    (
        select * 
        from phone_mapped
        where phone_decrypted <> '-' 
        and phone_decrypted is not null 
        and len(phone_decrypted) >= 10 
        and lead_date >= '2020-01-01'
    ) AS A

    LEFT JOIN sch_mapped AS B 
    ON A.phone_decrypted::varchar = B.unbounce_phone::varchar ANd A.lead_date::date = B.lead_date::date
    LEFT JOIN txn_mapped AS C
    ON A.phone_decrypted::varchar = C.unbounce_phone::varchar ANd A.lead_date::date = C.lead_date::date
)

select * from ml_funnel
;