--drop table temp.stg_all_leads_funnel_segment
create table dm.experian_scrub_report_202101_202106_18062021 as (
    with final as (
        with leads as (
            SELECT
                *
            from
                growth.stg_all_leads_funnel
            where
                lead_arrival_time :: date BETWEEN '2021-01-01'
                and '2021-06-17'
        ),
        segments as (
            select
                *
            from
                (
                    with data as (
                        select
                            phone_growth,
                            lead_arrival_time :: date as dot
                        from
                            growth.stg_all_leads_funnel
                        where
                            lead_arrival_time :: date BETWEEN '2021-01-01'
                            and '2021-06-17'
                    ),
                    exp_data1 as (
                        select
                            distinct customer_id as customerid,CASE
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
                            experian_scrub.ar_master
                        where
                            trim(open_dt) <> ''
                            and customerid in (
                                select
                                    distinct phone_growth
                                from
                                    data
                            )
                    ),
                    combined_data as (
                        with t2 as (
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
                    combined_data1 as (
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
                    select
                        a.*,case
                            when min_cred_date is null then 'NTC'
                            when min_gl_date is null then 'NTGL'
                            when gl_count :: float /(sub_count :: float + gl_count :: float) < 0.3 then 'LIGHT'
                            when gl_count :: float /(sub_count :: float + gl_count :: float) >= 0.3
                            and gl_count :: float /(sub_count :: float + gl_count :: float) < 0.7 then 'MEDIUM'
                            when gl_count :: float /(sub_count :: float + gl_count :: float) >= 0.7 then 'HEAVY'
                        end as cx_type
                    from
                        (
                            select
                                data.phone_growth,
                                data.dot,
                                min(open_date) as min_cred_date,
                                max(open_date) as max_cred_date,
                                min(
                                    case
                                        when acct_type_cd = '191' then open_date
                                    end
                                ) as min_gl_date,
                                count(
                                    distinct case
                                        when acct_type_cd = '191' then open_date
                                    end
                                ) as gl_count,
                                count(
                                    case
                                        when acct_type_cd in (
                                            '123',
                                            '189',
                                            '5',
                                            '176',
                                            '121',
                                            '184',
                                            '177',
                                            '185',
                                            '179',
                                            '220',
                                            '187',
                                            '175',
                                            '225',
                                            '228'
                                        ) then open_date
                                    end
                                ) as sub_count,
                                count(
                                    case
                                        when acct_type_cd in (
                                            '123',
                                            '189',
                                            '5',
                                            '176',
                                            '121',
                                            '184',
                                            '177',
                                            '185',
                                            '179',
                                            '220',
                                            '187',
                                            '175',
                                            '225',
                                            '228'
                                        )
                                        and m_sub_id = 'PVT' then open_date
                                    end
                                ) as pvt_sub_count,
                                count(
                                    distinct case
                                        when acct_type_cd = '191'
                                        and m_sub_id = 'PVT' then open_date
                                    end
                                ) as pvt_gl_count,
                                count(
                                    case
                                        when acct_type_cd in (
                                            '123',
                                            '189',
                                            '5',
                                            '176',
                                            '121',
                                            '184',
                                            '177',
                                            '185',
                                            '179',
                                            '220',
                                            '187',
                                            '175',
                                            '225',
                                            '228'
                                        )
                                        and m_sub_id = 'NBF' then open_date
                                    end
                                ) as nbf_sub_count,
                                count(
                                    distinct case
                                        when acct_type_cd = '191'
                                        and m_sub_id = 'NBF' then open_date
                                    end
                                ) as nbf_gl_count,
                                count(
                                    case
                                        when acct_type_cd in (
                                            '123',
                                            '189',
                                            '5',
                                            '176',
                                            '121',
                                            '184',
                                            '177',
                                            '185',
                                            '179',
                                            '220',
                                            '187',
                                            '175',
                                            '225',
                                            '228'
                                        )
                                        and m_sub_id = 'PUB' then open_date
                                    end
                                ) as pub_sub_count,
                                count(
                                    distinct case
                                        when acct_type_cd = '191'
                                        and m_sub_id = 'PUB' then open_date
                                    end
                                ) as pub_gl_count,
                                count(
                                    case
                                        when acct_type_cd in (
                                            '123',
                                            '189',
                                            '5',
                                            '176',
                                            '121',
                                            '184',
                                            '177',
                                            '185',
                                            '179',
                                            '220',
                                            '187',
                                            '175',
                                            '225',
                                            '228'
                                        )
                                        and m_sub_id = 'OTHERS' then open_date
                                    end
                                ) as others_sub_count,
                                count(
                                    distinct case
                                        when acct_type_cd = '191'
                                        and m_sub_id = 'OTHERS' then open_date
                                    end
                                ) as others_gl_count,
                                case
                                    when (gl_count + sub_count) = 0 then 'MIXED'
                                    when (pvt_sub_count + pvt_gl_count) /(gl_count + sub_count) > 0.7 then 'PVT'
                                    when (pub_sub_count + pub_gl_count) /(gl_count + sub_count) > 0.7 then 'PUB'
                                    when (nbf_sub_count + nbf_gl_count) /(gl_count + sub_count) > 0.7 then 'NBF'
                                    else 'MIXED'
                                end as cx_institute
                            from
                                data
                                left join combined_data1 on data.phone_growth = combined_data1.customerid
                                and data.dot > combined_data1.open_date
                            group by
                                1,
                                2
                        ) a
                ) Base1
                left join (
                    select
                        phone_growth_1,
                        lead_arrival_time,
                        count(
                            case
                                when m_sub_id = 'PVT' then open_dt
                            end
                        ) as pvt_next_30,
                        count(
                            case
                                when m_sub_id = 'PUB' then open_dt
                            end
                        ) as pub_next_30,
                        count(
                            case
                                when m_sub_id = 'NBF' then open_dt
                            end
                        ) as NBF_next_30,
                        count(open_dt) as total_next_30
                    from
                        (
                            select
                                *
                            from
                                (
                                    with t1 as (
                                        select
                                            distinct phone_growth as phone_growth_1,
                                            lead_arrival_time :: date as lead_arrival_time,
                                            lead_arrival_time :: date + INTERVAL '30 days' as next_30_days
                                        from
                                            growth.stg_all_leads_funnel
                                        where
                                            lead_arrival_time :: date BETWEEN '2021-01-01'
                                            and '2021-06-17'
                                    ),
                                    t2 as (
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
                                left join (
                                    with data as (
                                        select
                                            phone_growth,
                                            lead_arrival_time :: date as dot
                                        from
                                            growth.stg_all_leads_funnel
                                        where
                                            lead_arrival_time :: date BETWEEN '2021-01-01'
                                            and '2021-06-17'
                                    ),
                                    exp_data1 as (
                                        select
                                            distinct customer_id as customerid,CASE
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
                                            experian_scrub.ar_master
                                        where
                                            trim(open_dt) <> ''
                                            and customerid in (
                                                select
                                                    distinct phone_growth
                                                from
                                                    data
                                            )
                                    ),
                                    combined_data as (
                                        with t2 as (
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
                                    combined_data1 as (
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
                                ) b on a.phone_growth_1 = b.customerid
                                and b.open_dt > a.lead_arrival_time
                                and b.open_dt <= a.next_30_days
                        ) b1
                    group by
                        1,
                        2
                ) Base2 on Base1.phone_growth = Base2.phone_growth_1
                and Base1.dot :: date = Base2.lead_arrival_time
        )
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
            --cx_type,
            pvt_next_30,
            pub_next_30,
            nbf_next_30,
            total_next_30
        from
            leads
            left join segments on leads.phone_growth = segments.phone_growth
            and leads.lead_arrival_time :: date = segments.dot :: date
    )
    SELECT
      distinct decrypt_mobile_string(lead_phone) as lead_phone_dec,final.*,
        case
            when customer_id is not null then cx_type
            else 'Not Scrubbed'
        end as cx_type_new
    from
        final
        left join experian_scrub.ar_master b on final.lead_phone = b.customer_id
)
;

select lead_phone_dec
, case when lower(TRIM(source)) like '%google%' then 'brand' when lower(TRIM(source)) like '%website%' then 'website' else lower(TRIM(source)) end as channel
, datepart(year,lead_arrival_time)::int*100+datepart(month,lead_arrival_time)::int as mnth
, lead_arrival_time::date as lead_date
, lower(TRIM(city)) as city
, campaign_growth
, customer_type
, transaction_type
, cashtransferred_date as txn_date
, case when cashtransferred_date is not null then 1 else 0 end as transaction_completed
, cx_type_new
from dm.experian_scrub_report_202101_202106_18062021
where (lower(TRIM(source)) in ('app', 'organic', 'website') or (lower(TRIM(source)) like '%google%' and lower(campaign_growth) = 'brand') or lower(TRIM(source)) like '%website%')
and city is not null
;