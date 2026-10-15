with hourly_date_range as (
  select min(date) as min_date
  from `__PROJECT__.booming_data_analytics.stg_facebook_ads_hourly`
),

daily as (
  select
    date(time, "America/Los_Angeles") as date,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(spend) as spend
  from `__PROJECT__.booming_data_analytics.stg_facebook_ads`
  cross join hourly_date_range
  where date(time, "America/Los_Angeles") >= hourly_date_range.min_date
    and date(time, "America/Los_Angeles") < date_sub(
      current_date("America/Los_Angeles"),
      interval 3 day
    )
  group by date
),

hourly as (
  select
    date,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(spend) as spend
  from `__PROJECT__.booming_data_analytics.stg_facebook_ads_hourly`
  where date < date_sub(
    current_date("America/Los_Angeles"),
    interval 3 day
  )
  group by date
),

differences as (

select
  coalesce(daily.date, hourly.date) as date,
  daily.impressions as daily_impressions,
  hourly.impressions as hourly_impressions,
  daily.clicks as daily_clicks,
  hourly.clicks as hourly_clicks,
  daily.spend as daily_spend,
  hourly.spend as hourly_spend
from daily
full outer join hourly
  using (date)
where daily.date is null
   or hourly.date is null
   or abs(daily.impressions - hourly.impressions) > greatest(
     10,
     abs(daily.impressions) * 0.0001
   )
   or abs(daily.clicks - hourly.clicks) > greatest(
     1,
     abs(daily.clicks) * 0.0001
   )
   or abs(daily.spend - hourly.spend) > greatest(
     0.50,
     abs(daily.spend) * 0.0001
   )

)
select
  cast(date as string) as date,
  daily_impressions, hourly_impressions,
  daily_clicks, hourly_clicks,
  cast(daily_spend as string) as daily_spend,
  cast(hourly_spend as string) as hourly_spend,
  cast(daily_spend - hourly_spend as string) as spend_difference
from differences
where to_hex(sha256(to_json_string(differences))) not in (__ACCEPTED_FINGERPRINTS__)

order by date desc
