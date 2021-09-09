with vars as
(
    select 
    '2021-01-01' as start_date
    , '2021-07-31' as end_date
)
,

leads as
-- Leads data from leads table
(
    select mobile_number_decrypted
    , phone_growth
    , lead_arrival_date
    , city
    , campaign
    , campaign_growth
    , customer_type
    , transaction_type
    , txn_date
    , next_30_days::date as next_30_days

    from
    (
        SELECT distinct
        primaryphonenumber_decrypted as mobile_number_decrypted
        , phone_growth
        , lead_arrival_time
        , lead_arrival_time::date as lead_arrival_date
        , lower(city_growth) as city
        , campaign
        , campaign_growth
        , customer_type
        , transaction_type
        , cashtransferred_date as txn_date
        , lead_arrival_time :: date + INTERVAL '30 days' as next_30_days
        , row_number() over (partition by phone_growth, lead_arrival_time::date order by lead_arrival_time desc) as rnum
        
        from
        growth.stg_all_leads_funnel
        where lower(source) like '%google%'
        and lead_arrival_time :: date BETWEEN (select start_date from vars) and (select end_date from vars)
    )
    where rnum = 1
)
,

experian_data as
-- Get experian data from experian scrub table(s)
(
    select distinct
    customer_id_decrypted as customerid,
    
    CASE
    WHEN trim(open_dt) = '' THEN NULL
    ELSE TO_DATE(open_dt, 'yyyy-mm-dd')
    END AS open_dt,

    acct_type_cd,

    case
    when m_sub_id in ('PVT', 'PUB', 'NBF') then m_sub_id
    else 'Others'
    end as m_sub_id,

    CASE
    WHEN trim(closed_dt) = ''
    or closed_dt is null THEN TO_DATE('01/01/2030', 'yyyy-mm-dd')
    ELSE TO_DATE(closed_dt, 'yyyy-mm-dd')
    END AS closed_date,

    CASE
    WHEN trim(orig_loan_am) = '' THEN 0
    WHEN lower(orig_loan_am) LIKE '%e%' then 1000000
    ELSE orig_loan_am :: BIGINT
    END AS orig_loan_am1

    from
    (
        select * from experian_scrub.ar_master
    )

    where customer_id_decrypted in (select distinct mobile_number_decrypted from leads)
)
,

experian_renewal_tagged as 
-- Adding Renewal loan tag
(
    -- Gold loan users
    with t2 as
    (
        SELECT
        DISTINCT customerid,
        closed_date
        from
        experian_data
        where
        acct_type_cd = '191'
    )

    SELECT
    distinct experian_data.*,
    case
    when t2.customerid is not null then 1
    else 0
    end as renewal_tag
    from experian_data
    left join t2
    on experian_data.customerid = t2.customerid
    and experian_data.open_dt :: date <= t2.closed_date :: date + INTERVAL '3 days'
    and experian_data.open_dt :: date >= t2.closed_date :: date
)
,

experian_data_renewaL_removed as
-- Removing renewal gold loans
-- Union of gold loans (after removing renewals and summing up all renewal amounts) and non gold loans
(
    select
    customerid,
    open_dt,
    acct_type_cd,
    m_sub_id,
    sum(orig_loan_am1) as orig_loan_am1
    from
    experian_renewal_tagged
    where
    acct_type_cd = '191'
    and renewal_tag = 0
    group by
    1,
    2,
    3,
    4

    union all
    select
    customerid,
    open_dt,
    acct_type_cd,
    m_sub_id,
    orig_loan_am1 as orig_loan_am1
    from
    experian_renewal_tagged
    where
    acct_type_cd != '191'
)
,

leads_segment_history as
-- Credit history and segment tagging for leads (Before lead arrival date)
-- Segment tagging (NTC/ NTGL/ Light / Medium / Heavy)
(
    select
    experian_history.*
    , case
    when min_cred_date is null then 'NTC'
    when min_gl_date is null then 'NTGL'
    when gl_count :: float /(sub_count :: float + gl_count :: float) < 0.3 then 'LIGHT'
    when gl_count :: float /(sub_count :: float + gl_count :: float) >= 0.3
    and gl_count :: float /(sub_count :: float + gl_count :: float) < 0.7 then 'MEDIUM'
    when gl_count :: float /(sub_count :: float + gl_count :: float) >= 0.7 then 'HEAVY'
    end as cx_type_scrubbed
    from
    (
        -- Taggings for all leads to be scrubbed
        -- Credit history (For all loans before lead date)
        select
        leads.mobile_number_decrypted,
        leads.lead_arrival_date,
        min(open_dt) as min_cred_date,
        max(open_dt) as max_cred_date,
        min(case when acct_type_cd = '191' then open_dt end) as min_gl_date,
        count(distinct case when acct_type_cd = '191' then open_dt end) as gl_count,
        count( case when acct_type_cd in ('123', '189', '5', '176', '121', '184', '177', '185', '179', '220', '187', '175', '225', '228') then open_dt end) as sub_count,
        count(case when acct_type_cd in ('123','189','5','176','121','184','177','185','179','220','187','175','225','228') and m_sub_id = 'PVT' then open_dt end) as pvt_sub_count,
        count(distinct case when acct_type_cd = '191' and m_sub_id = 'PVT' then open_dt end) as pvt_gl_count,
        count(case when acct_type_cd in ('123','189','5','176','121','184','177','185','179','220','187','175','225','228') and m_sub_id = 'NBF' then open_dt end) as nbf_sub_count,
        count(distinct case when acct_type_cd = '191' and m_sub_id = 'NBF' then open_dt end) as nbf_gl_count,
        count(case when acct_type_cd in ('123','189','5','176','121','184','177','185','179','220','187','175','225','228') and m_sub_id = 'PUB' then open_dt end) as pub_sub_count,
        count(distinct case when acct_type_cd = '191' and m_sub_id = 'PUB' then open_dt end) as pub_gl_count,
        count(case when acct_type_cd in ('123','189','5','176','121','184','177','185','179','220','187','175','225','228') and m_sub_id = 'OTHERS' then open_dt end) as others_sub_count,
        count(distinct case when acct_type_cd = '191' and m_sub_id = 'OTHERS' then open_dt end) as others_gl_count,
        case
        when (gl_count + sub_count) = 0 then 'MIXED'
        when (pvt_sub_count + pvt_gl_count) /(gl_count + sub_count) > 0.7 then 'PVT'
        when (pub_sub_count + pub_gl_count) /(gl_count + sub_count) > 0.7 then 'PUB'
        when (nbf_sub_count + nbf_gl_count) /(gl_count + sub_count) > 0.7 then 'NBF'
        else 'MIXED'
        end as cx_institute
        
        from leads
        
        left join experian_data_renewaL_removed
        on leads.mobile_number_decrypted = experian_data_renewaL_removed.customerid
        and leads.lead_arrival_date > experian_data_renewaL_removed.open_dt
        group by 1,2
    ) experian_history
)
,

rupeek_first_transactions as 
-- Minimum transaction date for each lead in Rupeek's DB
(
    select
    DISTINCT phone, 
    min(
        case
        when cashtransferred is null then checkouttime :: date
        else cashtransferred :: date
        end
    ) :: date as cashtransferred_date
    FROM
    dw.core_loanrequest
    INNER JOIN dw.core_user ON dw.core_loanrequest.requesterid = dw.core_user.id
    WHERE
    statuscode BETWEEN 3.5
    AND 8
    group by
    1
)
,

completed_txns_mapped as
-- Leads data with completed txns tagged
(
    SELECT DISTINCT
    leads.*,
    case
    when cashtransferred_date is null then 'NO'
    else 'YES'
    end as completed
    from
    leads
    left join rupeek_first_transactions on leads.phone_growth = rupeek_first_transactions.phone
    and leads.lead_arrival_date <= cashtransferred_date::date
)
,

experian_data_for_gl_post_lead as
-- Experian data for gold loans post lead arrival date
(
    select
    customerid,
    open_dt,
    m_sub_id
    from
    experian_renewal_tagged
    where acct_type_cd = '191'
    and renewal_tag = '0'
)
,

segments_post_lead_arrival as
-- Mapping segments and taggings post lead arrival date
(
    select
    mobile_number_decrypted
    , lead_arrival_date
    , count(case when m_sub_id = 'PVT' then open_dt end) as pvt_next_30
    , count(case when m_sub_id = 'PUB' then open_dt end) as pub_next_30
    , count(case when m_sub_id = 'NBF' then open_dt end) as NBF_next_30
    , count(open_dt) as total_gl_next_30_days

    from completed_txns_mapped a
    left join experian_data_for_gl_post_lead b
    on a.mobile_number_decrypted = b.customerid
    and b.open_dt > a.lead_arrival_date
    and b.open_dt <= a.next_30_days

    group by 1,2
)
,

lead_segments_pre_post_mapped as
-- Mapping leads credit history and gold loan taken post lead arrival
(
    select distinct
    a.*
    , pvt_next_30
    , pub_next_30
    , NBF_next_30
    , total_gl_next_30_days

    from leads_segment_history a
    left join segments_post_lead_arrival b
    on a.mobile_number_decrypted = b.mobile_number_decrypted
    and a.lead_arrival_date = b.lead_arrival_date
)
,

all_segments_mapped as
-- Merging leads with segments data 
(
    SELECT
    leads.*,
    min_cred_date,
    max_cred_date,
    gl_count,
    sub_count,
    pvt_sub_count,
    pvt_gl_count,
    nbf_sub_count,
    nbf_gl_count,
    pub_sub_count,
    pub_gl_count,
    others_sub_count,
    others_gl_count,
    cx_institute,
    cx_type_scrubbed,
    pvt_next_30,
    pub_next_30,
    nbf_next_30,
    total_gl_next_30_days
    from leads
    left join lead_segments_pre_post_mapped segments
    on leads.mobile_number_decrypted = segments.mobile_number_decrypted
    and leads.lead_arrival_date = segments.lead_arrival_date
)

-- Adding experian data availability tag
SELECT distinct
all_segments_mapped.*
, case when customerid is not null then cx_type_scrubbed else 'Not Scrubbed' end as customer_segment
from all_segments_mapped
left join experian_data on all_segments_mapped.mobile_number_decrypted = experian_data.customerid
;