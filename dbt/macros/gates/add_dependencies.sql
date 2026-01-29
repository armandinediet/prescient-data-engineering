{% macro add_dependencies(model_names) %}
  {% if not model_names or model_names | length == 0 %}
    {% do exceptions.raise_compiler_error(
      "add_dependencies(model_names) requires a non-empty list of model names"
    ) %}
  {% endif %}

  {# Create real DAG edges without affecting query output #}
  {% for model_name in model_names %}
    {% set _ = ref(model_name) %}
  {% endfor %}
{% endmacro %}