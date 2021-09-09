----Total Leads

with tmp as 
(
SELECT 
	--- Change the date for the required time period ------
	'2021-04-04'::DATE AS dt,
	-------------------------------------------------------
	'some string'      AS some_value,
	5556::BIGINT       AS some_id
),
scrub AS (
select mobile_num as mobile
from growth.gs_txn_teardown_new
where lower(status) ='completed'
and date >='2020-02-01'
),

lead as 
(
select 
            distinct lead_phone as mob_no, 
            max (lead_arrival_time::date) over(partition by lead_phone) as Lead_date,
            datepart(month,lead_arrival_time::date) as Lead_Month,City 
			from growth.stg_all_leads_funnel
			where (lower(source) like '%fb%' or lower(source) like '%facebook%')
			),
			
txn_cancelled as

(

	SELECT distinct id, mobile_num as Phone_4, name,status as Txn_Status,date as txn_date
	,datepart(month,date)as Txn_Month, cancellation_reason_comments, final_amount::int as loan_amt
	, type,city as txn_city,'Direct'::text as a_b, on_reach_cancellation
	from growth.gs_txn_teardown_new
	where date >= (select dt from tmp)
	and lower(status) in ('cancelled')
	union
	(
		SELECT distinct id,a_b_mobile, name,status as Txn_Status,date as txn2_date
		,datepart(month,date)as Txn_Month, cancellation_reason_comments
		,final_amount::int,type,city as txn_city,'A_b'::text as a_b,on_reach_cancellation
		from growth.gs_txn_teardown_new
		where date >= (select dt from tmp) 
		and lower(status) in ('cancelled')
	)
)

select *
from
(
	select distinct decrypt_mobile_string(Phone_4),a.*,orc_after_customer_meet_or_before_customer_meet_,orc_date::date,what_is_the_reaons_for_cancellation_,
	customer_issue,audit_done_by_,internal_issue,lender_issue,mc_call_done_by_,orc_after_customer_meet_or_before_customer_meet_,trust_issue
	, row_number() over (partition by decrypt_mobile_string order by txn_date desc) as rnk
	from txn_cancelled a
	left join growth.gs_orc_form_response_three 
	on a.phone_4 = customer_number and orc_date::date = a.txn_date
	inner join lead on mob_no =Phone_4
	where (txn_date - lead_date between 0 and 60)
	and txn_date >=(select dt from tmp)
	and  (Phone_4) not in (select mobile from scrub where (scrub.mobile) = (Phone_4))
)
where rnk = 1
;