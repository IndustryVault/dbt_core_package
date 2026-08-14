{% macro validate_share(share_name, db_name, exclude_schemas=['INFORMATION_SCHEMA','ARTIFACTS']) %}
  {% if execute %}

    {% set excl = "'" ~ exclude_schemas | join("','") ~ "'" %}

    {# 1. objects that SHOULD be shared, across ALL schemas: tables + secure views #}
    {#    keyed as SCHEMA.OBJECT so same-named objects in different schemas don't collide #}
    {% set expected_sql %}
      SELECT table_schema || '.' || table_name AS obj
      FROM {{ db_name }}.INFORMATION_SCHEMA.TABLES
      WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ({{ excl }})
      UNION
      SELECT table_schema || '.' || table_name AS obj
      FROM {{ db_name }}.INFORMATION_SCHEMA.VIEWS
      WHERE is_secure = 'YES'
        AND table_schema NOT IN ({{ excl }})
    {% endset %}
    {% set expected = run_query(expected_sql).columns[0].values() %}

    {# 2. objects actually granted to the share (schema.object) #}
    {% do run_query("SHOW GRANTS TO SHARE " ~ share_name) %}
    {% set shared_sql %}
      SELECT SPLIT_PART("name", '.', 2) || '.' || SPLIT_PART("name", '.', 3) AS obj
      FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
      WHERE "granted_on" IN ('TABLE','VIEW','MATERIALIZED_VIEW')
    {% endset %}
    {% set shared = run_query(shared_sql).columns[0].values() %}

    {# 3. diff #}
    {% set missing = [] %}
    {% for o in expected %}
      {% if o not in shared %}{% do missing.append(o) %}{% endif %}
    {% endfor %}

    {% if missing | length > 0 %}
      {{ exceptions.raise_compiler_error(
          "Objects missing from share '" ~ share_name ~ "' ("
          ~ missing | length ~ "): " ~ missing | join(', ')) }}
    {% else %}
      {{ log("All " ~ expected | length
             ~ " tables/secure views across all schemas present in share '"
             ~ share_name ~ "'.", info=True) }}
    {% endif %}

  {% endif %}
{% endmacro %}
