{% macro test_not_null(model, column_name, where=None) %}
select
  '{{ model }}' as model_name,
  'not_null__{{ column_name }}' as test_name,
  count(*) as validation_errors
from {{ model }}
where {{ column_name }} is null
{% if where %} and ({{ where }}) {% endif %}
{% endmacro %}

{% macro test_unique(model, column_name, where=None) %}
select
  '{{ model }}' as model_name,
  'unique__{{ column_name }}' as test_name,
  count(*) as validation_errors
from (
  select {{ column_name }}
  from {{ model }}
  {% if where %} where ({{ where }}) {% endif %}
  group by 1
  having count(*) > 1
) d
{% endmacro %}
