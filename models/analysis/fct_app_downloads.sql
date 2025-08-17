{{
    config(
        materialized = 'table',
        pk = ['date', 'platform']
    )
}}
with
cte_android as (
    select
        date,
        'Android' as platform,
        sum(daily_device_installs) as downloads,
        max(convert_timezone('America/Los_Angeles', _fivetran_synced)) as source_synced_ts,
        current_timestamp() as inserted_ts
    from {{source('google_play', 'stats_installs_overview')}}
    where
        package_name = 'com.truereligion.app.truereligion'
        and daily_device_installs > 0
        and date >= '2023-01-29'
    group by all
),
cte_ios_historical as (
    select
        date,
        'iOS' as platform,
        sum(total_downloads) as downloads_historical,
        max(convert_timezone('America/Los_Angeles', inserted_ts)) as source_synced_ts_historical
    from {{source('load', 'itunes_connect_downloads')}}
    where
        date >= '2023-01-29'
    group by all
),
cte_ios_fivetran as (
    select
        date,
        'iOS' as platform,
        sum(counts) as downloads_fivetran,
        max(convert_timezone('America/Los_Angeles', _fivetran_synced)) as source_synced_ts_fivetran
    from {{source('itunes_connect', 'app_store_download_standard_daily')}}
    where
        download_type in ('First-time download', 'Redownload')
        and counts > 0
        and date >= '2023-01-29'
    group by all
)
select
    date,
    platform,
    downloads,
    source_synced_ts,
    current_timestamp() as inserted_ts
from cte_android

union all

select
    date,
    platform,
    coalesce(downloads_fivetran, downloads_historical) as downloads,
    coalesce(source_synced_ts_fivetran, source_synced_ts_historical) as source_synced_ts,
    current_timestamp() as inserted_ts
from cte_ios_historical
full join cte_ios_fivetran using (date, platform)
order by date, platform