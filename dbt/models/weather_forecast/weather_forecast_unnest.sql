{{ config(
    materialized="table",
    schema='staging'
    ) }}

with src as (
  select
    id as raw_event_id,
    request_id,
    run_id,
    requested_at,
    status_code,
    error,
    request_url,
    request_params,
    extra_meta,
    flatten_payload
  from {{ source("raw", "raw_weather_forecast") }}
  where job_name = 'openweather_forecast'
    and flatten_payload is not null
)

select
  -- identifiers / lineage
  raw_event_id,
  request_id,
  run_id,
  requested_at as ingested_at,

  -- optional request metadata
  status_code,
  error,

  -- city metadata (stored in extra_meta)
  (extra_meta->'city'->>'id')::int as city_id,
  extra_meta->'city'->>'name' as city_name,
  extra_meta->'city'->>'country' as country,
  (extra_meta->'city'->'coord'->>'lat')::double precision as lat,
  (extra_meta->'city'->'coord'->>'lon')::double precision as lon,
  (extra_meta->'city'->>'timezone')::int as timezone_offset_sec,

  -- forecast fields (flatten_payload dotted keys)
  (flatten_payload->>'dt')::bigint as forecast_dt_unix,
  (flatten_payload->>'dt_txt')::timestamp as forecast_dt_txt,

  (flatten_payload->>'main.temp')::double precision as temp,
  (flatten_payload->>'main.feels_like')::double precision as feels_like,
  (flatten_payload->>'main.temp_min')::double precision as temp_min,
  (flatten_payload->>'main.temp_max')::double precision as temp_max,
  (flatten_payload->>'main.pressure')::int as pressure,
  (flatten_payload->>'main.humidity')::int as humidity,
  (flatten_payload->>'clouds.all')::int as clouds_all,
  (flatten_payload->>'visibility')::int as visibility,
  (flatten_payload->>'wind.speed')::double precision as wind_speed,
  (flatten_payload->>'wind.deg')::int as wind_deg,
  (flatten_payload->>'wind.gust')::double precision as wind_gust,
  (flatten_payload->>'pop')::double precision as pop,
  (flatten_payload->>'rain.3h')::double precision as rain_3h,
  (flatten_payload->>'snow.3h')::double precision as snow_3h,
  (flatten_payload->>'sys.pod') as pod,

  -- weather is still an array in flatten_payload; take the first element (index 0)
  ((flatten_payload->'weather'->0)->>'id')::int as weather_id,
  (flatten_payload->'weather'->0)->>'main' as weather_main,
  (flatten_payload->'weather'->0)->>'description' as weather_description,
  (flatten_payload->'weather'->0)->>'icon' as weather_icon

from src
