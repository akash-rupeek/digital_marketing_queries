select 'dm_leads' as tablename, max(TIMESTAMP) as mt
from growth.dm_leads
where lower(utmsource) like '%google%'

union

select 'chakra_leads_fresh' as tablename, max(created_at) as mt
from dw.chakra_leads_fresh
where lower(lead_source) like '%google%'

union

select 'stg_all_leads_funnel' as tablename, max(lead_arrival_time) as mt
from growth.stg_all_leads_funnel
where lower(source) like '%google%'
;

select count(DISTINCT(phone_number_decrypted)) as leads
from dw.chakra_leads_fresh
where lower(lead_source) like '%google%'
and created_at::date = getdate()::date
;

select 'dm_leads' as tablename, count(DISTINCT(phone_number_decrypted)) as leads
from growth.dm_leads
where lower(utmsource) like '%google%'
and TIMESTAMP::date = getdate()::date

union

select 'chakra_leads_fresh' as tablename, count(DISTINCT(phone_number_decrypted)) as leads
from dw.chakra_leads_fresh
where lower(lead_source) like '%google%'
and created_at::date = getdate()::date

union

select 'stg_all_leads_funnel' as tablename, count(DISTINCT(phone_number_decrypted)) as leads
from growth.stg_all_leads_funnel
where lower(source) like '%google%'
and lead_arrival_time::date = getdate()::date
;