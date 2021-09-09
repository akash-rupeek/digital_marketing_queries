----Total Leads

with tmp as 
(
SELECT 
	--- Change the date for the required time period ------
	'2020-10-01'::DATE AS dt,
	-------------------------------------------------------
	'some string'      AS some_value,
	5556::BIGINT       AS some_id
),

scrub AS 
(
	select mobile_num as mobile
	from growth.gs_txn_teardown_new
	where lower(status) ='completed'
	and date >='2020-02-01'
),

lead as 
(
	select *
	from
	(
		select 
	    distinct lead_phone as mob_no
	    , lead_arrival_time::date as Lead_date
	    , datepart(month,lead_arrival_time::date) as Lead_Month
	    , lower(city) as city
	    , customer_type
	    , pushed_date::DATE as pushed_date
	    , preferred_date::DATE as preferred_date
	    , scheduled_for_date::DATE as scheduled_for_date
	    , cx_type
	    , row_number() over (partition by lead_phone order by pushed_date desc) as rownum
		from growth.stg_all_leads_funnel
		where (lower(source) like '%fb%' or lower(source) like '%facebook%')
	) stg
	where rownum = 1
),
			
txn_cancelled as
(
	SELECT distinct id
	, mobile_num as Phone_4
	, name
	, status as Txn_Status
	, date as txn_date
	, datepart(month, date) as Txn_Month
	, cancellation_reason_comments
	, final_amount::int as loan_amt
	, type
	, city as txn_city
	, 'Direct'::text as a_b
	, on_reach_cancellation
	from growth.gs_txn_teardown_new
	where date >= (select dt from tmp)
	--and lower(status) in ('cancelled')

	union

	SELECT distinct id
	, a_b_mobile
	, name
	, status as Txn_Status
	, date as txn2_date
	, datepart(month, date) as Txn_Month
	, cancellation_reason_comments
	, final_amount::int
	, type,city as txn_city
	, 'A_b'::text as a_b
	, on_reach_cancellation
	from growth.gs_txn_teardown_new
	where date >= (select dt from tmp)
	--and lower(status) in ('cancelled')
)

select decrypt_mobile_string as phone_number
--, name
, txn_date
, lower(Txn_Status) as txn_status
, datepart(year,txn_date)::int*100+datepart(month,txn_date)::int as txn_month
, datepart(year,txn_date)::int*100+datepart(week,txn_date)::int as txn_week
, lead_date
, pushed_date
, preferred_date
, cancellation_reason_comments
, loan_amt
, type
, txn_city
, on_reach_cancellation
, customer_type
, cx_type as customer_bucket
, orc_after_customer_meet_or_before_customer_meet_
, orc_date
, what_is_the_reaons_for_cancellation_
, customer_issue
, audit_done_by_
, internal_issue
, lender_issue
, mc_call_done_by_
, trust_issue
, case when pushed_date - lead_date = 0 then 'same_day_push'
when pushed_date - lead_date between 1 and 7 then 'same_week_push'
when pushed_date - lead_date > 7 then 'next_week_push' end as lead_to_push_date

, case when preferred_date is null then 'no_preferred_date'
	else case when preferred_date - pushed_date = 0 then 'D0_prefer'
		when preferred_date - pushed_date = 1 then 'D1_prefer'
		when preferred_date - pushed_date >= 2 then 'D2+_prefer'
		end
	end as pushed_and_preferred_date

, case when txn_date - pushed_date = 0 then 'D0_pushed'
	when txn_date - pushed_date = 1 then 'D1_pushed'
	when txn_date - pushed_date >= 2 then 'D2+_pushed'
	end as pushed_and_txn_date

, case when preferred_date is null then 'no_preferred_date'
	else case when txn_date - preferred_date = 0 then 'D0_prefer'
		when txn_date - preferred_date = 1 then 'D1_prefer'
		when txn_date - preferred_date >= 2 then 'D2+_prefer'
		end
	end as preferred_and_txn_date

from
(
	select distinct decrypt_mobile_string(Phone_4)
	, a.*
	, lead.*
	, orc_date::date
	, what_is_the_reaons_for_cancellation_
	, customer_issue
	, audit_done_by_
	, internal_issue
	, lender_issue
	, mc_call_done_by_
	, orc_after_customer_meet_or_before_customer_meet_
	, trust_issue
	, row_number() over (partition by decrypt_mobile_string order by txn_date desc) as rnk
	from txn_cancelled a
	left join growth.gs_orc_form_response_three b
	on a.phone_4 = customer_number and orc_date::date = a.txn_date
	inner join lead on mob_no =Phone_4
	where (txn_date - lead_date between 0 and 60)
	and txn_date >=(select dt from tmp)
	--and  (Phone_4) not in (select mobile from scrub where (scrub.mobile) = (Phone_4))
) b
where rnk = 1
;