{% macro execute_tests(fail_message="Pre-Release Validation Failed") -%}
  {# 
    Expects caller() SQL to return rows with:
      - test_name (string)
      - validation_errors (int)
    Optionally: model_name (string)
  #}

  {# During dbt compile, execute=False and run_query returns None #}
  {% if not execute %}
    select 'skipped (compile only)' as tests_result
  {% else %}

    {% set sql %}
      with checks as (
        {{ caller() }}
      )
      select
        coalesce(test_name, 'unknown') as test_name,
        coalesce(validation_errors, 0) as validation_errors
      from checks
    {% endset %}

    {% set table = run_query(sql) %}

    {% if table is none %}
      {% do exceptions.raise_compiler_error("execute_tests(): run_query returned none during execution") %}
    {% endif %}

    {% set col_names = table.column_names %}
    {% set idx_test = col_names.index('test_name') if 'test_name' in col_names else 0 %}
    {% set idx_err  = col_names.index('validation_errors') if 'validation_errors' in col_names else 1 %}

    {% set failing = [] %}
    {% set total = 0 %}

    {% for row in table.rows %}
      {% set tname = row[idx_test] %}
      {% set n = (row[idx_err] | int) %}
      {% set total = total + n %}
      {% if n > 0 %}
        {% do failing.append(tname ~ "=" ~ n) %}
      {% endif %}
    {% endfor %}

    {% if failing | length > 0 %}
      {% do exceptions.raise_database_error(
        fail_message ~ " | total_validation_errors=" ~ total ~ " | failing_tests: " ~ (failing | join(", "))
      ) %}
    {% endif %}

    select 'pass' as tests_result

  {% endif %}
{%- endmacro %}
