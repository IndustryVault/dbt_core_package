{% macro table_exists(database, schema, identifier) %}
  {% set source_relation = adapter.get_relation(
      database=database,
      schema=schema,
      identifier=identifier
  ) %}
  {{ return(source_relation is not none) }}
{% endmacro %}
