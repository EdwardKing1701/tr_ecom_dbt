{% macro udf() %}

create schema if not exists analysis;
use schema analysis;

create or replace function analytics_channel (default_channel_group text, medium text, source text, campaign_id text, campaign_name text, manual_ad_content text, source_platform text) returns text as
$$
case
    when
        default_channel_group = 'Paid Search'
        or medium ilike '%SA360%'
        or campaign_name ilike '%places%'
        or medium = 'cpc'
        or source = 'Apple'
    then 'Paid Search'

    when
        default_channel_group = 'Paid Shopping'
        and source <> 'google'
        and source <> 'bing'
    then 'Paid Shopping' -- These conditions negate each other. It was set up this way in the Google Analytics UI to effectively disable the channel

    when
        default_channel_group = 'Paid Video'
        or source = 'mntn'
    then 'Paid Video'

    when
        default_channel_group = 'Cross-network'
        and source <> 'google'
        and source <> 'bing'
    then 'Cross-network' -- These conditions negate each other. It was set up this way in the Google Analytics UI to effectively disable the channel

    when
        default_channel_group = 'Affiliates'
        or source = 'pjn'
    then 'Affiliates'

    when
        (default_channel_group = 'Paid Social'
            or lower(source) in ('tiktok', 'tiktok.com', 'ads.tiktok.com', 'facebook', 'facebook.com', 'instagram', 'ig', 'pinterest', 'reddit', 'snapchat', 'twitter', 'youtube', 'fragrance'))
        and (not medium in ('social', 'referral', 'post', 'organic')
            or medium = 'paid_social'
            or campaign_name like '%Snapchat%')
    then 'Paid Social'

    when
        default_channel_group = 'Display'
        or source in ('display', 'dv', 'dv360')
    then 'Display'

    when
        default_channel_group = 'Paid Other'
    then 'Paid Other'

    when
        default_channel_group = 'Email'
        or source ilike 'listr%'
        or medium ilike '%email%'
    then 'Email'

    when
        default_channel_group = 'Mobile Push Notifications'
        or source = 'airship'
    then 'Mobile Push Notifications'

    when
        default_channel_group = 'SMS'
        or source = 'attentive'
        or medium = 'text'
        or campaign_name in ('20241129_BFDoorbustersFH_NA_CAMP_PROMO_N_Engaged_PM', '20241128_BF48Hours_NA_CAMP_PROMO_N_ClickedPurchasedAllTime_PM', '20241128_BF48Hours_NA_CAMP_PROMO_N_Engaged_MID', '20241129_BFMMS_NA_CAMP_PROMO_N_HighIntent_Unengaged_MID', '20241129_BFDoorbusters_NA_CAMP_PROMO_N_Engaged_NO_MID', '20241129_BFDoorbusters_NA_CAMP_PROMO_N_Hasn''tReceived_MID', '20241128_BF48Hours_NA_CAMP_PROMO_N_Retargeting_PM', '20241129_BFDoorbusters_NA_CAMP_PROMO_N_Retargeting_PM', '20241129_BFMMS_NA_CAMP_PROMO_N_Engaged_NO_MID', '20250623_$59Shorts_NA_CAMP_PROMO_N_NTF_NO_AI', '20250622_$59DenimShortsEndsSoon_NA_CAMP_NTF_NO_AI', '20250622_OnlineExclusives_NA_CAMP_PROMO_N_180DPUR30DCLK_PM_base', '20250623_$59Denim_NA_CAMP_PROMO_N_180DPUR30DCLK_NO_PM_base', '20250622_$59DenimShortsEndsSoon_NA_CAMP_180DPUR30DCLK_NO_MID_base', '20250624_$59DenimShorts_NA_CAMP_PROMO_N_NTF_NO_AI_', '20250624_$59DenimShorts_NA_CAMP_PROMO_N_180DPUR30DCLK_NO_MID_base', '20250623_$59Shorts_NA_CAMP_PROMO_N_180DPUR30DCLK_NO_MID', '20250623_$59Denim_NA_CAMP_PROMO_N_180DPUR30DCLK_NO_PM_audiences_ai', '20250624_$59DenimShorts_NA_CAMP_PROMO_N_180DPUR30DCLK_NO_MID_audiences_ai')
        or source = 'myrlgn.attn.tv'
    then 'SMS'

    when
        source ilike '%pay%'
    then 'Payment Redirects'

    when
        default_channel_group = 'Organic Search'
    then 'Organic Search'

    when
        default_channel_group = 'Organic Shopping'
        and not campaign_name ilike '%header_shopnow%'
    then 'Organic Shopping'

    when
        default_channel_group = 'Organic Video'
    then 'Organic Video'

    when
        default_channel_group = 'Organic Social'
        or source ilike any ('curalate%', 'instagram%', 'youtube%', 'pinterest%', 'curalate_like2buy%', 'snap%', 'social%', 'hypebae%', 'face%', 'bv_%')
        or medium ilike any ('facebook', 'fb', 'organic%')
        or campaign_name like 'curalate_like2buy%'
    then 'Organic Social'

    when
        (default_channel_group = 'Referral'
            or source in ('dbm', 'chatgpt.com')
            or medium in ('hypebeast', 'landing_page'))
        and source not in ('curalate', 'curalate_like2buy')
    then 'Referral'

    when
        default_channel_group = 'Audio'
        or medium = 'spotify'
    then 'Audio'

    when
        default_channel_group = 'Direct'
        or source = 'connectedtv'
    then 'Direct'

    else 'Unassigned'

end
$$;

create or replace function hash_key(input array) returns text called on null input as
$$
select sha2(array_to_string(array_compact(input), ''))
$$;

create or replace function local_time(ts timestamp_tz) returns timestamp_tz strict immutable as
$$
select convert_timezone('America/Los_Angeles', ts)
$$;

create or replace function local_time(ts timestamp_ntz) returns timestamp_tz strict immutable as
$$
select convert_timezone('America/Los_Angeles', ts)
$$;

create or replace function phone_number(input text) returns text language javascript as
$$
if (INPUT === null || INPUT === undefined) {
    return null;
}

let input = INPUT.replace(/[^0-9]+/g, '').replace(/^[01]{1}/g, '');
if (input !== '' && input.length === 10 && parseInt(input.substring(0, 3)) >= 201) {
    return '+1' + input;
}

return null;
$$;

create or replace function rawurldecode(input text) returns text language javascript as
$$
if (INPUT === null || INPUT === undefined) {
    return null;
}

return decodeURIComponent(INPUT);
$$;

create or replace function email_address(input text) returns text as
$$
lower(iff(trim(input) rlike '^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$', trim(input), null))
$$;

{% endmacro %}