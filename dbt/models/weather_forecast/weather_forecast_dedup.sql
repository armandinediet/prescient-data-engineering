{{ config(
    materialized="view",
    schema="staging"
) }}

with src as (

  select
    *
  from {{ ref('weather_forecast_unnest') }}
  where city_id is not null
    and forecast_dt_txt is not null

),

dedup as (

  select
    *,
    row_number() over (
      partition by city_id, forecast_dt_txt
      order by
        ingested_at desc,
        raw_event_id desc
    ) as _rn
  from src

)

select
  -- lineage
  raw_event_id,
  request_id,
  run_id,
  ingested_at,

  -- city
  city_id,
  city_name,
  country,
  lat,
  lon,
  timezone_offset_sec,

  -- forecast keys
  forecast_dt_unix,
  forecast_dt_txt,

  -- metrics
  temp,
  feels_like,
  temp_min,
  temp_max,
  pressure,
  humidity,
  clouds_all,
  visibility,
  wind_speed,
  wind_deg,
  wind_gust,
  pop,
  rain_3h,
  snow_3h,
  pod,

  -- weather
  weather_id,
  weather_main,
  weather_description,
  weather_icon

from dedup
where _rn = 1
