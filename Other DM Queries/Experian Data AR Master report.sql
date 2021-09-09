CREATE TABLE dm.experian_report_202103_202105 AS

with leads as 
(
    select distinct unbounce_phone_decrypted as unbounce_phone
    , lead_date
    , max_cta_id
    , datepart(year,lead_date)::int*100+datepart(month,lead_date)::int as lead_month
    from dm.dr_mapped_leads
    where 
    -- Find and replace the lead month to get data for leads of required months 
    datepart(year,lead_date)::int*100+datepart(month,lead_date)::int in (202103,202104,202105)
),

-- Taking data of DM leads scrubs from respective tables according to scrub month

experian as 
(
    select distinct
    customer_id_decrypted as customerid,
    account_nb,
    m_sub_id,
    acct_type_cd,
    to_date(open_dt,'yyyy-mm-dd') as open_dt,
    actual_payment_am,
    asset_class_cd,
    balance_am,
    balance_dt::varchar,
    charge_off_am,
    credit_limit_am,
    days_past_due,
    dflt_status_dt,
    orig_loan_am
    from experian_scrub.ar_master
),

-- Loan share with each institute type out of total loans taken

dom_insti AS 
(
    select distinct right(customerid,10) as unbounce_phone, m_sub_id , insti_count::float/total::float AS insti_share
    from 
    (
        select customerid, m_sub_id
        ,count(*)::float as insti_count, count(*) over(partition by customerid) AS total
        , row_number() over(partition by customerid order by insti_count DESC) as prio
        from experian group by 1,2
    )

    where prio = 1
),

-- Transaction of leads in DM channel

conversion_rupeek as 
(
    select distinct unbounce_phone_decrypted as unbounce_phone, lead_date, sch_date, txn_date
    , customer_type, coalesce(txn_type, sch_type) as txn_type
    , final_amount, txn
    from dm.dr_mid_low_funnel_old_attr 
    where datepart(year,lead_date)::int*100+datepart(month,lead_date)::int in (202103,202104,202105)
),
     
-- Credit history amount for each lead before it came into the system

credit_hist_amt AS 
(
    select unbounce_phone, lead_date, sum(orig_loan_am) as credit_hist_amt
    from leads AS A
    LEFT JOIN experian AS B
    ON A.unbounce_phone = B.customerid 
    and A.lead_date > B.open_dt
    WHERE  acct_type_cd::int  in (123,189,5,176,121,184,177,185,179,220,187,175,225,228,191) 
    AND orig_loan_am is not null
    GROUP BY 1,2
),

-- Checking if this lead had transacted with rupeek earlier (For New vs Repeat Lead and Non NTGL tagging)

rpk_loan as
(
    select distinct unbounce_phone, lead_date, case when first_rpk_txn is null then 0 else 1 end as rpk_loan_flag
    from leads 
    left join 
    (select mobile_num_decrypted, min(date) as first_rpk_txn from growth.gs_txn_teardown_new where lower(status) = 'completed' group by 1) as rpk_txn
    on leads.unbounce_phone = rpk_txn.mobile_num_decrypted and leads.lead_date > rpk_txn.first_rpk_txn
),

-- Checking if this lead had taken a GL from some other company before it came to Rupeek (For Non NTGL tagging)

other_gold_loan as 
(
    select distinct unbounce_phone, lead_date, case when first_open_dt is null then 0 else 1 end as other_gold_loan_flag
    from leads 
    left join 
    (
        SELECT RIGHT(customerid,10) customerid, min(to_date(open_dt,'yyyy-mm-dd')) as first_open_dt
        FROM experian
        WHERE  lower(m_sub_id) not like  '%rupeek%' AND acct_type_cd  = 191
        GROUP BY 1
    ) as other_txn
    on leads.unbounce_phone = other_txn.customerid and leads.lead_date > other_txn.first_open_dt
),

-- Checking if credit history exists for this lead before it came to rupeek (to create NTC tagging)

credit_hist as 
(
    select distinct unbounce_phone, lead_date, case when first_open_dt is null then 0 else 1 end as credits_flag
    from leads 
    left join 
    (
        SELECT RIGHT(customerid,10) customerid, min(to_date(open_dt,'yyyy-mm-dd')) as first_open_dt
        FROM experian
        GROUP BY 1
    ) as other_txn
    on leads.unbounce_phone = other_txn.customerid and leads.lead_date > other_txn.first_open_dt
),

-- Checking how frequently the user takes GL in comparison to gold loan substitutes

gold_frequency as 

(
    select  unbounce_phone, lead_date
    , SUM(coalesce(gold_loans,0)) AS gold_loans
    , SUM(coalesce(gold_subst_loans,0)) AS gold_subst_loans
    , CASE WHEN SUM(coalesce(gold_subst_loans,0)) = 0 THEN '0' 
    WHEN SUM(coalesce(gold_loans,0))::float/SUM(coalesce(gold_subst_loans,0))::float between 0.00000003 and 0.3 THEN 'Light'
    WHEN SUM(coalesce(gold_loans,0))::float/SUM(coalesce(gold_subst_loans,0))::float between 0.3 and 0.7 THEN 'Medium'
    WHEN SUM(coalesce(gold_loans,0))::float/SUM(coalesce(gold_subst_loans,0))::float > 0.7 THEN 'Heavy'
    END AS gold_frequency_flag
    from leads 

    left join 

    (
        SELECT RIGHT(customerid,10) AS mob_no
        , to_date(open_dt,'yyyy-mm-dd') as open_dt
        , case when acct_type_cd::int = 191 THEN 1 ELSE 0 END AS gold_loans
        , case when acct_type_cd::int in (123,189,5,176,121,184,177,185,179,220,187,175,225,228,191) THEN 1 ELSE 0 END AS gold_subst_loans
        FROM experian
        WHERE  acct_type_cd::int  in (123,189,5,176,121,184,177,185,179,220,187,175,225,228,191) AND orig_loan_am is not null
        GROUP BY 1,2,3,4
    ) AS user_loan_bucket
    ON leads.unbounce_phone::varchar = user_loan_bucket.mob_no::varchar AND leads.lead_date >= user_loan_bucket.open_dt 
    group by 1,2
),

-- Check if user is taking high value loans

affluent as 
(
    select  unbounce_phone, lead_date 
    , CASE WHEN sum(coalesce(affluent_flag,0)) >= 1 THEN 1 ELSE 0 END AS Affluent_flag
    from leads 
    left join 
    (
        SELECT RIGHT(customerid,10) AS mob_no
        ,to_date(open_dt,'yyyy-mm-dd') as open_dt
        , max(case when acct_type_cd::int = 213 THEN 1                              --Corporate Credit Card                                
        when acct_type_cd::int = 58 THEN 1                                          --Instalment Loan, Mortgage
        when acct_type_cd::int = 47 AND orig_loan_am::float > 100000 THEN 1         --Instalment Loan, Automobile >1L
        when acct_type_cd::int = 187 THEN 1                                         --Loan To Professional
        when acct_type_cd::int = 189 AND orig_loan_am::float > 30000 THEN 1         --Loan, Consumer >30k
        when acct_type_cd::int = 195 THEN 1                                         --Loan, Property
        when acct_type_cd::int = 123 AND orig_loan_am::float > 500000 THEN 1        --Loan, Personal Cash >5l
        when acct_type_cd::int = 221 THEN 1                                         --Used Car Loan
        ELSE 0 END) AS Affluent_flag
        FROM experian
        WHERE  acct_type_cd  in (213,58, 47, 187, 189, 195, 123, 221)
        GROUP BY 1,2 
    ) AS Affluent
    ON leads.unbounce_phone = Affluent.mob_no AND leads.lead_date > Affluent.open_dt
    group by 1,2
),

-- Checking if the user is taking gold loan somewhere else after lead date

next_gl AS 
(
    select distinct unbounce_phone, lead_date, other_txn_dt, m_sub_id
    , case when other_txn_dt < '2100-01-01' and other_txn_dt is not null then 1 else 0 end as other_txn
    from 
    (
        select distinct unbounce_phone, lead_date,coalesce(open_dt,'2100-01-01') as other_txn_dt , m_sub_id
        , row_number() over(partition by unbounce_phone,lead_date order by other_txn_dt ASC) AS row_prio
        from leads
        left join 
        (
            select distinct right(customerid,10) as customerid, open_dt, m_sub_id 
            from experian 
            where acct_type_cd::int = 191 and lower(m_sub_id) not like  '%rupeek%'
        ) as experian 
        ON leads.unbounce_phone = experian.customerid 
        and leads.lead_date <= experian.open_dt 
    )
    where row_prio = 1
),

-- Checking if user is taking some other loan somewhere else (GL or Non GL)
-- If someone is taking Non GL first and then GL both will have different entry
-- If someone is taking a GL first after the lead date then both next_gl and next_loan will be same
-- If someone is taking a GL first and then taking another loan then other loan data will not be available

next_loan AS 
(
    select distinct unbounce_phone, lead_date, other_txn_dt as next_loan_dt,  m_sub_id as next_loan_m_sub_id , acct_type_cd as nxt_loan_acct_type_cd
    , case when other_txn_dt < '2100-01-01' and other_txn_dt is not null then 1 else 0 end as other_txn
    from 
    (
        select distinct unbounce_phone, lead_date,coalesce(open_dt,'2100-01-01') as other_txn_dt , m_sub_id, acct_type_cd
        ,row_number() over(partition by unbounce_phone,lead_date order by other_txn_dt ASC) AS row_prio
        from leads
        left join
        (
            select distinct right(customerid,10) as customerid, open_dt, m_sub_id , acct_type_cd
            from experian 
            where  lower(m_sub_id) not like  '%rupeek%'
        ) as experian 
        ON leads.unbounce_phone = experian.customerid 
        and leads.lead_date <= experian.open_dt 
    )
    where row_prio = 1
),


experian_data as
(
    SELECT leads.*, B.type, B.city_abr, B.channel_group, B.sem_bau_bucket, B.channel_grouping1, B.bau_sub_buckets, B.campaign_bucket
    , case when credits_flag = 0 and other_gold_loan_flag = 0 and rpk_loan_flag = 0 and Affluent_flag = 0 THEN 'NTC'
    when credits_flag = 1 and other_gold_loan_flag = 0 and rpk_loan_flag = 0 and Affluent_flag = 0 THEN 'NTGL Non  - Affluent'
    when credits_flag = 1 and other_gold_loan_flag = 0 and rpk_loan_flag = 0 and Affluent_flag = 1 THEN 'NTGL Affluent'
    ELSE 'Non NTGL' END AS customer_type_flag
    , case when C.mob_no is null then 0 ELSE 1 END as exp_data_flag
    , dom_insti.m_sub_id as dom_sub, dom_insti.insti_share
    , credit_hist_amt.credit_hist_amt AS credit_hist_amt
    , cnv.sch_date, cnv.txn_date, cnv.customer_type, cnv.txn as rpk_converted, cnv.txn_type
    , rpk_loan.rpk_loan_flag
    , other_gold_loan.other_gold_loan_flag
    , gold_frequency.gold_frequency_flag
    , credit_hist.credits_flag
    , affluent.affluent_flag
    , next_gl.other_txn, next_gl.other_txn_dt, next_gl.m_sub_id
    , next_loan.next_loan_dt ,  next_loan.next_loan_m_sub_id, next_loan.nxt_loan_acct_type_cd, case when next_loan.next_loan_dt  < '2100-01-01' THEN 1 ELSE 0 END as next_loan
    , dispo.max_disposition, dispo.max_sub_disposition
    , cnv.final_amount

    from 
    leads

    LEFT JOIN dm.dr_mapped_leads AS B
    ON leads.unbounce_phone = B.unbounce_phone_decrypted and leads.lead_date = B.lead_date
    LEFT JOIN dom_insti
    ON leads.unbounce_phone = dom_insti.unbounce_phone 

    LEFT JOIN credit_hist_amt
    ON leads.unbounce_phone = credit_hist_amt.unbounce_phone and leads.lead_date = credit_hist_amt.lead_date

    LEFT JOIN conversion_rupeek as cnv
    ON leads.unbounce_phone = cnv.unbounce_phone and leads.lead_date = cnv.lead_date

    LEFT JOIN rpk_loan
    ON leads.unbounce_phone = rpk_loan.unbounce_phone and leads.lead_date = rpk_loan.lead_date

    LEFT JOIN other_gold_loan
    ON leads.unbounce_phone = other_gold_loan.unbounce_phone and leads.lead_date = other_gold_loan.lead_date

    LEFT JOIN gold_frequency
    ON leads.unbounce_phone = gold_frequency.unbounce_phone and leads.lead_date = gold_frequency.lead_date

    LEFT JOIN credit_hist
    ON leads.unbounce_phone = credit_hist.unbounce_phone and leads.lead_date = credit_hist.lead_date

    LEFT JOIN affluent
    ON leads.unbounce_phone = affluent.unbounce_phone and leads.lead_date = affluent.lead_date

    LEFT JOIN next_gl
    ON leads.unbounce_phone = next_gl.unbounce_phone and leads.lead_date = next_gl.lead_date

    LEFT JOIN next_loan
    ON leads.unbounce_phone = next_loan.unbounce_phone and leads.lead_date = next_loan.lead_date

    LEFT JOIN (SELECT distinct RIGHT(customerid,10) AS mob_no FROM experian) AS C
    ON leads.unbounce_phone = C.mob_no 

    LEFT JOIN 
    (
        select distinct unbounce_phone, lead_date, max_sub_disposition, max_disposition 
        from dm.dr_disposition_data 
        where 
        --lead_date between (select start_date from startend)         and (select end_date from startend)

        datepart(year,lead_date)::int*100+datepart(month,lead_date)::int in (202103,202104,202105)
    ) AS dispo
    ON leads.unbounce_phone = dispo.unbounce_phone and leads.lead_date = dispo.lead_date
),

experian_scrub_data as
(
    select distinct
    unbounce_phone
    , lead_date
    , lead_month
    , sch_date
    , txn_date
    , city_abr
    , campaign_bucket
    , sem_bau_bucket
    , bau_sub_buckets
    , txn_type as loan_type
    , customer_type
    , case when customer_type_flag in ('NTGL Affluent', 'NTGL Non  - Affluent') then 'NTGL' else customer_type_flag end as customer_profile
    , gold_frequency_flag
    , exp_data_flag as data_available_in_experian
    , case when rpk_converted = 1 then 1 else 0 end rupeek_converters
    , case when rpk_converted = 1 or other_txn = 1 or next_loan = 1 then 1 else 0 end as loan_seekers
    , case when rpk_converted = 0 and other_txn = 1 then 1 else 0 end as non_conv_taken_gl
    , case when rpk_loan_flag = 0 and ((other_txn = 0 and next_loan = 1) or (other_txn = 1 and next_loan = 1 and other_txn_dt <> next_loan_dt)) then 1 else 0 end as non_conv_taken_other_loan
    , case when rpk_converted = 1 then 'rupeek_converters'
       when rpk_converted = 0 and other_txn = 1 then 'non_conv_taken_gl'
       when rpk_converted = 0 and other_txn = 0 and next_loan = 1 then 'non_conv_taken_other_loan'
       else 'non_loan_seekers' end as loan_tag
    from experian_data
),

call_data as
(
    select distinct customer_ph_no_decrypted
    , date_time as call_timestamp
    , id
    , date_time::DATE as call_date
    , call_status
    , call_duration_secs
    , actual_talk_time_secs
    from dw.c_zentrix_calls
    where customer_ph_no_decrypted in (select distinct unbounce_phone from experian_scrub_data)
    and datepart(year,date_time)::int*100+datepart(month,date_time)::int in (202103,202104,202105)
),

call_durations as
(
    select customer_ph_no_decrypted
    , min(call_timestamp) as first_calL_timestamp
    , min(call_date) as first_called_date
    , max(call_date) as last_called_date
    , count(distinct(call_date)) as no_of_days_called
    , count(distinct(id)) as total_attempts
    , count(distinct(case when call_status in ('answered', 'transfer', 'DNC') then id else null end)) as successful_attempts
    , sum(call_duration_secs) as total_call_duration
    , sum(actual_talk_time_secs) as total_talktime
    , count(distinct(case when actual_talk_time_secs < 30 then id else null end)) as Below30_sec_calls
    , count(distinct(case when actual_talk_time_secs between 30 and 60 then id else null end)) as BW_30and60_sec_calls
    , count(distinct(case when actual_talk_time_secs between 60 and 120 then id else null end)) as BW_60and120_sec_calls
    , count(distinct(case when actual_talk_time_secs > 120 then id else null end)) as Above120_sec_calls
    from call_data
    group by customer_ph_no_decrypted
)

select *
from
(
    select experian_scrub_data.*, call_durations.*
    , case when customer_ph_no_decrypted is not null then 1 else 0 end as called
    from experian_scrub_data
    left join call_durations
    on experian_scrub_data.unbounce_phone = call_durations.customer_ph_no_decrypted
) leads_scrubbed
;