{# DAG anchors (always parsed) #}
{% set _ = ref('weather_forecast_canonical') %}

{% call execute_tests("Pre-release failed for canonical_weather") %}

  {{ test_not_null(ref('weather_forecast_canonical'), 'unique_id') }}
  union all
  {{ test_unique(ref('weather_forecast_canonical'), 'unique_id') }}

{% endcall %}
