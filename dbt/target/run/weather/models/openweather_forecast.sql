
  create view "weather"."public"."openweather_forecast__dbt_tmp"
    
    
  as (
    

select
  city_id,
  city_name,
  country,
  lat,
  lon,
  timezone_offset_sec,
  forecast_dt_unix,
  forecast_dt_utc,
  temp,
  feels_like,
  temp_min,
  temp_max,
  pressure,
  humidity,
  clouds_all,
  wind_speed,
  wind_deg,
  wind_gust,
  visibility,
  pop,
  rain_3h,
  snow_3h,
  weather_id,
  weather_main,
  weather_description,
  weather_icon,
  ingestion_id,
  ingested_at
from "weather"."public"."stg_openweather_forecast"
  );