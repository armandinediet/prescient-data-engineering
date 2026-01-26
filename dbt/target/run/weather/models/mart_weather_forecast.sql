
  create view "weather"."public"."mart_weather_forecast__dbt_tmp"
    
    
  as (
    

select
  city_id,
  city_name,
  country,
  forecast_dt_utc,
  temp,
  feels_like,
  humidity,
  pressure,
  wind_speed,
  clouds_all,
  coalesce(rain_3h, 0) as rain_3h,
  coalesce(snow_3h, 0) as snow_3h,
  weather_main,
  weather_description,
  case
    when temp is null then null
    else (temp * 9.0/5.0) + 32.0
  end as temp_fahrenheit
from "weather"."public"."openweather_forecast"
  );