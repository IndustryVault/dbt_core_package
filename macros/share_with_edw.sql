{% macro share_with_edw() %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% for schema in schemas if schema | upper not in ['ARTIFACTS'] %}
        grant usage on database {{ target.database }} to share SDW_{{ target.database }};
        grant usage on schema {{ target.database }}.{{ schema }} to share SDW_{{target.database}};
        grant select on all tables in schema {{ target.database }}.{{ schema }} to share SDW_{{ target.database }};

        {%- set secured_views %}
            select table_catalog as DATABASE, table_schema AS SCHEMA, table_name AS VIEW
            from {{ target.database }}.information_schema.views
            where is_secure='YES' and table_schema = '{{ schema | upper }}'
        {%- endset -%}

        {%- set views = run_query(secured_views) %}    
        {%- for v in views %}
            grant select on view {{ v.DATABASE }}.{{ v.SCHEMA }}.{{ v.VIEW }} to share SDW_{{ v.DATABASE }};
        {%- endfor -%}

        {% do log(" - Sharing schema: " ~ schema, info=True) %}
    {% endfor %}
{% endmacro %}
