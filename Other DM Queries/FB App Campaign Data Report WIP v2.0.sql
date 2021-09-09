with vars as
(
    select 
    '2021-06-01' as start_date
    , current_date - INTEGER '1' AS end_date
    --, '2021-04-04' as end_date
    , 45 as app_attribution_window
    , 14 as others_attribution_window
),

top_funnel as 
(
    SELECT campaign_date
    , campaign_name
    , sum(impressions) as impressions   
    , sum(inline_link_clicks) as clicks     
    , sum(spend) as spends  
    from 
    (
        SELECT distinct campaign_date
        , campaign_name
        , impressions   
        , inline_link_clicks    
        , reach 
        , spend 
        , actions   
        , campaign_source   
        , cc_leads  
        , leads_cc  
        , cc_purchase   
        , cc_view_content
        from dm.facebook_add_data
    )a 

    where lower(campaign_name) in (select distinct lower(campaign_name) from dm.facebook_uac_app_campaign_temp)
    and campaign_date >= (select start_date from vars)

    group by campaign_date
    , campaign_name
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
    where lower(last_attributed_touch_data_tilde_campaign) in (select distinct lower(campaign_name) from dm.facebook_uac_app_campaign_temp)
),


-- Mapping mobile numbers to different AAIDs got from branch.
-- Mapping campaign and install dates

phone_mapped as
(
    select branch.*, cuser.*
    , core_user_lead_timestamp::date as core_user_date
    -- , datediff(day, install_timestamp, core_user_lead_timestamp) as install_to_lead_diff
    from
    (   
        select id, leadid, phone_decrypted, phones_decrypted, phone
        , case when lead_date_rank = 1 then created_at else null end as core_user_lead_timestamp
        from
        (
            select distinct id, leadid, phone_decrypted, phones_decrypted, created_at
            , rank() over (partition by phone_decrypted order by created_at desc) as lead_date_rank
            from dw.core_user
        ) cuser_
    ) cuser

    inner join

    (
        select aaid, devid, channel, campaign
        , listagg(city, ' / ') as cities
        , min(install_timestamp) as install_timestamp
        from
        (
            select distinct user_data_aaid as aaid
            , user_data_developer_identity as devid
            , last_attributed_touch_data_tilde_channel as channel
            , last_attributed_touch_data_tilde_campaign as campaign
            , case when lower(user_data_geo_city_en) like '%delhi%' then 'Delhi' else user_data_geo_city_en end as city
            , install_activity_timestamp_iso as install_timestamp
            from branch_data
            where user_data_aaid is not null
        ) branch_

        group by 1,2,3,4
    ) branch on cuser.id = branch.devid

),


-- Getting leads from app and other campaigns
-- If app lead date < other lead date or (app lead date is not null and other lead date is null) then it's an app paid campaign lead


all_leads as
(
    select phone_number_decrypted as phone, created_at as lead_timestamp, lower(source) as source, lower(utm_source) as utmsource
    from dw.chakra_leads_fresh
    where created_at::date >= (select start_date from vars)
    and phone in (select distinct phone_decrypted from phone_mapped)
),

app_leads as
(
    select phone
    , lead_timestamp
    , source as app_source
    , case when lower(source) = 'app' then 1 else 2 end as priority 
    from all_leads
    where lower(source) like '%app%'
    and (lower(utmsource) like '%regular%' or lower(utmsource) is null)
),

non_app_leads as
(
    select *
    from all_leads
    where lower(source) not like '%app%' 
),

app_leads_mapped as
(
    select phone_mapped.*, 
    from phone_mapped
    left join app_leads on phone_mapped.phone_decrypted = app_leads.phone and phone_mapped.install_timestamp <= last_app_lead_date
)

