with vars as
(
    select 
    '2021-06-01' as start_date
    , current_date - INTEGER '1' AS end_date
    --, '2021-04-04' as end_date
    , 45 as app_attribution_window
    , 14 as others_attribution_window
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
    where last_attributed_touch_data_tilde_campaign in (
        'UAC - Bangalore - Mysuru - June 2021', 'UAC - Delhi - NCR - June 2021'
        , 'Exp App Campaign Del June', 'Exp App Campaign BLR June'
        )
),


-- Mapping mobile numbers to different AAIDs got from branch.
-- Mapping campaign and install dates

phone_mapped as
(
    select branch.*, cuser.*
    , core_user_lead_timestamp::date as core_user_lead_date
    , datediff(day, install_timestamp, core_user_lead_timestamp) as install_to_lead_diff
    from
    (   
        select id, leadid, phone_decrypted, phones_decrypted
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


-- Getting lead arrival history for all leads coming via paid app campaign.
-- Any lead that has lead arrival post installation of app and within attribution
--    window for app (default = 45 days) will get attributed to paid app campaigns.
-- Lead that have arrival history before installation date will get an attribution window (default = 14 days).
-- Lead coming before 14 days (or attribution window for other leads) of install will get attributed to paid app else to that source.


master_leads as 
(
    select distinct arrival_time as lead_arrival_time
    , mobile_number_decrypted as mobile
    , source.name as source
    , campaign.name as campaign
    , channel.name as channel
    from 
    (
        select distinct arrival_time, mobile_number_decrypted, source_id, campaign_id, channel_id
        from dw.leads_master
        where mobile_number_decrypted in (select distinct phone_decrypted from phone_mapped)
    ) leads
    left join dw.source_master source on leads.source_id = source.id
    left join dw.campaign_master campaign on leads.campaign_id = campaign.id
    left join dw.channel_master channel on leads.channel_id = channel.id
),


-- Storing all leads that came before the install date within the atrribution window (default = 14 days).
-- These leads won't be allocated to Paid App campaign.
-- App leads removed as they would be from paid app campaigns only


leads_before_install as 
(
    select mobile
    , case when lead_rank_by_mobile = 1 then source else null end as last_source_before_install
    , case when lead_rank_by_mobile = 1 then lead_arrival_time else null end as last_lead_before_install
    , listagg('{' || source || '_' || campaign || '_' || channel || '_' || lead_arrival_time || '_' || lead_arrival_and_install_diff, ' / ') as other_leads
    from
    (
        select mleads.*
        , install_timestamp, datediff(day, lead_arrival_time, install_timestamp) as lead_arrival_and_install_diff
        , rank() over (partition by mobile, mleads.source order by lead_arrival_time desc) lead_rank_by_source -- To use only 1 lead value per source
        , rank() over (partition by mobile order by lead_arrival_time desc) lead_rank_by_mobile -- For latest lead source before install time
        from master_leads as mleads
        join phone_mapped pmapped
        on mleads.mobile = pmapped.phone_decrypted
        where lead_arrival_time < install_timestamp
        and datediff(day, lead_arrival_time, install_timestamp) between 0 and (select others_attribution_window from vars)
        and lower(mleads.channel) <> 'app'
    ) min_max_leads
    where lead_rank_by_source = 1
    and source is not null
    group by 1,2,3
)

select * from leads_before_install

;