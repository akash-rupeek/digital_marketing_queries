with vars as
(
    select 
    '2021-03-20' AS start_date
    , current_date - INTEGER '1' AS end_date
    --, '2021-04-04' as end_date
),

dm_leads as
(
	select distinct unbounce_phone_decrypted
	, min(lead_timestamp) as lead_timestamp
	, min(campaign_name) as campaign_name
	, max(max_cta_id) as max_cta_id
	, min(lead_date) as lead_date
	from dm.dr_mid_low_funnel_old_attr
	where lower(sem_bau_bucket) = 'facebook'
	and lead_date between (select start_date from vars) and (select end_date from vars)
	and unbounce_phone_decrypted not in ('6111111111', '9999999999')
	and sch = 0 and txn = 0
	group by unbounce_phone_decrypted
),

call_data as
(
	select customer_ph_no_decrypted
	, date_time as call_timestamp
	, id
	, date_time::DATE as call_date
	, call_status
	, cust_disposition
	, call_duration_secs
	, actual_talk_time_secs
	from dw.c_zentrix_calls
	where customer_ph_no_decrypted in (select distinct unbounce_phone_decrypted from dm_leads)
	and date_time::DATE between (select start_date from vars) and (select end_date from vars)
),

call_durations as
(
	select customer_ph_no_decrypted
	, min(call_timestamp) as first_calL_timestamp
	, min(call_date) as first_called_date
	, max(call_date) as last_called_date
	, count(distinct(call_date)) as days_called
	, count(id) as total_attempts
	, count(case when call_status in ('answered', 'transfer', 'DNC') then id else null end) as successful_attempts
	, sum(call_duration_secs) as total_call_duration
	, sum(actual_talk_time_secs) as total_talktime
	, count(case when actual_talk_time_secs < 30 then id else null end) as Below30_sec_calls
	, count(case when actual_talk_time_secs between 30 and 60 then id else null end) as BW_30and60_sec_calls
	, count(case when actual_talk_time_secs between 60 and 120 then id else null end) as BW_60and120_sec_calls
	, count(case when actual_talk_time_secs > 120 then id else null end) as Above120_sec_calls
	from call_data
	group by customer_ph_no_decrypted
)

select *
, case when tat < 300 then 1 else 0 end as tat_less_then_5_min
, extract(week from lead_date) as weeknum
from
(
	select dm_leads.*, call_durations.*
	, case when customer_ph_no_decrypted is not null then 1 else 0 end as called
	, extract(epoch from (first_calL_timestamp::timestamp - dm_leads.lead_timestamp::timestamp)) as tat
	from dm_leads
	left join call_durations
	on dm_leads.unbounce_phone_decrypted = call_durations.customer_ph_no_decrypted
) xx

;