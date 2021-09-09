drop table dm.experian_report_2021_26082021;
create table dm.experian_report_2021_26082021 as 
(
    -- Leads with scrubbed data
    with final as 
    (
        -- Get Leads list for scrubbing
        with leads as 
        (
            SELECT
            *
            from
            growth.stg_all_leads_funnel
            where
            lead_arrival_time :: date BETWEEN '2020-11-01'
            and '2021-02-28'
        ),

        -- Credit history at the moment of lead arrival with loan taken in next 30 days after lead arrival 
        segments as
        (
            -- Credit history and segment tagging for leads (Before lead arrival date)
            select
            *
            from
            (
                -- Get Leads list for scrubbing
                with data as 
                (
                    select
                    phone_growth,
                    lead_arrival_time :: date as dot
                    from
                    growth.stg_all_leads_funnel
                    where
                    lead_arrival_time :: date BETWEEN '2020-11-01'
                    and '2021-02-28'
                ),

                -- Get experian data from experian scrub table for leads which need to be scrubbed
                exp_data1 as 
                (
                    select
                    distinct customerid,
                    
                    CASE
                    WHEN trim(open_dt) = '' THEN NULL
                    ELSE TO_DATE(open_dt, 'dd/mm/yyyy')
                    END AS open_date,
                    acct_type_cd,

                    case
                    when m_sub_id in ('PVT', 'PUB', 'NBF') then m_sub_id
                    else 'Others'
                    end as m_sub_id,

                    CASE
                    WHEN trim(closed_dt) = ''
                    or closed_dt is null THEN TO_DATE('01/01/2030', 'dd/mm/yyyy')
                    ELSE TO_DATE(closed_dt, 'dd/mm/yyyy')
                    END AS closed_date,

                    CASE
                    WHEN trim(orig_loan_am) = '' THEN 0
                    WHEN lower(orig_loan_am) LIKE '%e%' then 1000000
                    ELSE orig_loan_am :: BIGINT
                    END AS orig_loan_am1
                    
                    from
                    experian_scrub."experian_2019-20_all_leads_feb20_scrub"
                    where
                    trim(open_dt) <> ''
                    and customerid in (select distinct phone_growth from data)
                ),

                -- Adding Renewal loan tag
                combined_data as 
                (
                    -- Gold loan users
                    with t2 as
                    (
                        SELECT
                        DISTINCT customerid,
                        closed_date
                        from
                        exp_data1
                        where
                        acct_type_cd = '191'
                    )

                    SELECT
                    distinct exp_data1.*,
                    case
                    when t2.customerid is not null then 1
                    else 0
                    end as renewal_tag
                    from
                    exp_data1
                    left join t2 on exp_data1.customerid = t2.customerid
                    and exp_data1.open_date :: date <= t2.closed_date :: date + INTERVAL '3 days'
                    and exp_data1.open_date :: date >= t2.closed_date :: date
                ),

                -- Removing renewal gold loans
                -- Union of gold loans (after removing renewals and summing up all renewal amounts) and non gold loans
                combined_data1 as
                (
                    select
                    customerid,
                    open_date,
                    acct_type_cd,
                    m_sub_id,
                    sum(orig_loan_am1) as orig_loan_am1
                    from
                    combined_data
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
                    open_date,
                    acct_type_cd,
                    m_sub_id,
                    orig_loan_am1 as orig_loan_am1
                    from
                    combined_data
                    where
                    acct_type_cd != '191'
                )

                -- Credit history and segment tagging for leads (Before lead arrival date)
                -- Segment tagging (NTC/ NTGL/ Light / Medium / Heavy)
                select
                a.*
                , case
                when min_cred_date is null then 'NTC'
                when min_gl_date is null then 'NTGL'
                when gl_count :: float /(sub_count :: float + gl_count :: float) < 0.3 then 'LIGHT'
                when gl_count :: float /(sub_count :: float + gl_count :: float) >= 0.3
                and gl_count :: float /(sub_count :: float + gl_count :: float) < 0.7 then 'MEDIUM'
                when gl_count :: float /(sub_count :: float + gl_count :: float) >= 0.7 then 'HEAVY'
                end as cx_type
                from
                (
                    -- Taggings for all leads to be scrubbed
                    -- Credit history (For all loans before lead date)
                    select
                    data.phone_growth,
                    data.dot,
                    min(open_date) as min_cred_date,
                    max(open_date) as max_cred_date,
                    min(case when acct_type_cd = '191' then open_date end) as min_gl_date,
                    count(distinct case when acct_type_cd = '191' then open_date end) as gl_count,
                    count( case when acct_type_cd in ('123', '189', '5', '176', '121', '184', '177', '185', '179', '220', '187', '175', '225', '228') then open_date end) as sub_count,
                    count(case when acct_type_cd in ('123','189','5','176','121','184','177','185','179','220','187','175','225','228') and m_sub_id = 'PVT' then open_date end) as pvt_sub_count,
                    count(distinct case when acct_type_cd = '191' and m_sub_id = 'PVT' then open_date end) as pvt_gl_count,
                    count(case when acct_type_cd in ('123','189','5','176','121','184','177','185','179','220','187','175','225','228') and m_sub_id = 'NBF' then open_date end) as nbf_sub_count,
                    count(distinct case when acct_type_cd = '191' and m_sub_id = 'NBF' then open_date end) as nbf_gl_count,
                    count(case when acct_type_cd in ('123','189','5','176','121','184','177','185','179','220','187','175','225','228') and m_sub_id = 'PUB' then open_date end) as pub_sub_count,
                    count(distinct case when acct_type_cd = '191' and m_sub_id = 'PUB' then open_date end) as pub_gl_count,
                    count(case when acct_type_cd in ('123','189','5','176','121','184','177','185','179','220','187','175','225','228') and m_sub_id = 'OTHERS' then open_date end) as others_sub_count,
                    count(distinct case when acct_type_cd = '191' and m_sub_id = 'OTHERS' then open_date end) as others_gl_count,
                    case
                    when (gl_count + sub_count) = 0 then 'MIXED'
                    when (pvt_sub_count + pvt_gl_count) /(gl_count + sub_count) > 0.7 then 'PVT'
                    when (pub_sub_count + pub_gl_count) /(gl_count + sub_count) > 0.7 then 'PUB'
                    when (nbf_sub_count + nbf_gl_count) /(gl_count + sub_count) > 0.7 then 'NBF'
                    else 'MIXED'
                    end as cx_institute
                    
                    from data
                    
                    left join combined_data1 on data.phone_growth = combined_data1.customerid
                    and data.dot > combined_data1.open_date
                    group by
                    1,
                    2
                ) a
            ) Base1

            left join

            -- Leads data with window of next 30 days for loans with other companies
            (
                -- Leads data with window of next 30 days for loans with other companies
                select
                phone_growth_1,
                lead_arrival_time,
                count(case when m_sub_id = 'PVT' then open_dt end) as pvt_next_30,
                count(case when m_sub_id = 'PUB' then open_dt end) as pub_next_30,
                count(case when m_sub_id = 'NBF' then open_dt end) as NBF_next_30,
                count(open_dt) as total_next_30
                from
                (
                    -- Rupeek txn data with experian data for next 30 days window
                    select
                    *
                    from

                    -- Completed txns for leads
                    -- next_30_days column with lead date + 30 days window
                    (
                        -- Leads data for scrubbing with an interval window of next 30 days
                        with t1 as 
                        (
                            select
                            distinct phone_growth as phone_growth_1,
                            lead_arrival_time :: date as lead_arrival_time,
                            lead_arrival_time :: date + INTERVAL '30 days' as next_30_days
                            from
                            growth.stg_all_leads_funnel
                            where
                            lead_arrival_time :: date BETWEEN '2020-11-01'
                            and '2021-02-28'
                        ),

                        -- Minimum transaction date for each lead in Rupeek's DB
                        t2 as 
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

                        -- Completed transactions for leads
                        SELECT
                        DISTINCT t1.*,case
                        when cashtransferred_date is null then 'NO'
                        else 'YES'
                        end as completed
                        from
                        t1
                        left join t2 on t1.phone_growth_1 = t2.phone
                        and t1.lead_arrival_time :: date <= cashtransferred_date :: date
                    ) a

                    left join 

                    -- Experian data with renewal GL removed
                    (
                        -- Leads for scrubbing
                        with data as 
                        (
                            select
                            phone_growth,
                            lead_arrival_time :: date as dot
                            from
                            growth.stg_all_leads_funnel
                            where
                            lead_arrival_time :: date BETWEEN '2020-11-01'
                            and '2021-02-28'
                        ),

                        -- Experian data for leads that need to be scrubbed
                        exp_data1 as 
                        (
                            select
                            distinct customerid,CASE
                            WHEN trim(open_dt) = '' THEN NULL
                            ELSE TO_DATE(open_dt, 'dd/mm/yyyy')
                            END AS open_date,
                            acct_type_cd,case
                            when m_sub_id in ('PVT', 'PUB', 'NBF') then m_sub_id
                            else 'Others'
                            end as m_sub_id,CASE
                            WHEN trim(closed_dt) = ''
                            or closed_dt is null THEN TO_DATE('01/01/2030', 'dd/mm/yyyy')
                            ELSE TO_DATE(closed_dt, 'dd/mm/yyyy')
                            END AS closed_date,CASE
                            WHEN trim(orig_loan_am) = '' THEN 0
                            WHEN lower(orig_loan_am) LIKE '%e%' then 1000000
                            ELSE orig_loan_am :: BIGINT
                            END AS orig_loan_am1
                            from
                            experian_scrub."experian_2019-20_all_leads_feb20_scrub"
                            where
                            trim(open_dt) <> ''
                            and customerid in (
                            select
                            distinct phone_growth
                            from
                            data
                            )
                        ),

                        -- Adding renewal loan tags
                        combined_data as 
                        (
                            -- Gold loan users from experian table
                            with t2 as 
                            (
                                SELECT
                                DISTINCT customerid,
                                closed_date
                                from
                                exp_data1
                                where
                                acct_type_cd = '191'
                            )

                            SELECT
                            distinct exp_data1.*,
                            case
                            when t2.customerid is not null then 1
                            else 0
                            end as renewal_tag
                            from
                            exp_data1
                            left join t2 on exp_data1.customerid = t2.customerid
                            and exp_data1.open_date :: date <= t2.closed_date :: date + INTERVAL '3 days'
                            and exp_data1.open_date :: date >= t2.closed_date :: date
                        ),

                        -- Removing reneal gold loans
                        combined_data1 as 
                        (
                            select
                            customerid,
                            open_date as open_dt,
                            acct_type_cd,
                            renewal_tag,
                            m_sub_id,
                            sum(orig_loan_am1) as orig_loan_am1
                            from
                            combined_data
                            where
                            acct_type_cd = '191'
                            group by
                            1,
                            2,
                            3,
                            4,
                            5
                            union all
                            select
                            customerid,
                            open_date,
                            acct_type_cd,
                            renewal_tag,
                            m_sub_id,
                            orig_loan_am1 as orig_loan_am1
                            from
                            combined_data
                            where
                            acct_type_cd != '191'
                        )

                        select
                        customerid,
                        open_dt,
                        m_sub_id
                        from
                        combined_data1
                        where
                        acct_type_cd = '191'
                        and renewal_tag = '0'
                    ) b
                    on a.phone_growth_1 = b.customerid
                    and b.open_dt > a.lead_arrival_time
                    and b.open_dt <= a.next_30_days
                ) b1
                group by
                1,
                2
            ) Base2 
            on Base1.phone_growth = Base2.phone_growth_1
            and Base1.dot :: date = Base2.lead_arrival_time
        )
        
        -- Merging leads with segments data 
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
        cx_type,
        pvt_next_30,
        pub_next_30,
        nbf_next_30,
        total_next_30
        from
        leads
        left join segments on leads.phone_growth = segments.phone_growth
        and leads.lead_arrival_time :: date = segments.dot :: date
    )

    -- Check if leads are available in experian scrub or not
    SELECT
    distinct decrypt_mobile_string(lead_phone) as lead_phone_dec
    , final.*
    , case when customerid is not null then cx_type else 'Not Scrubbed' end as cx_type_new
    from
    final
    left join experian_scrub."experian_2019-20_all_leads_feb20_scrub" b on final.lead_phone = b.customerid
)
