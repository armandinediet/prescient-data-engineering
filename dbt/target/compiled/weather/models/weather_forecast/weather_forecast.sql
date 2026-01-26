

select
  -- grain: one row per city + forecast timestamp
  city_id,
  city_name,
  country,
  lat,
  lon,
  timezone_offset_sec,

  forecast_dt_unix,
  forecast_dt_txt,

  -- core weather metrics
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

  weather_id,
  weather_main,
  weather_description,
  weather_icon,

  -- lineage / freshness
  ingested_at

from "weather"."staging"."weather_forecast_dedup"