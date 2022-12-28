{% macro generate_from_dictionary_source_yml(database_name='default', version_name='default', is_external=false, apply_filter='') %}

    {% set sources_yaml=[] %}

    {% if database_name=='default' %}
        {% set database_name = var('dictionary_database', target.database) %}
    {% endif %}
    {% if version_name=='default' %}
        {% set version_name = var('dictionary_database_version', 'default') %}
    {% endif %}

    {% set header %}
#
#   This file was originally generated by the macro generate_from_dictionary_source_yml and is now under manual control. Replacing with a regenerated file will lose changes!!
#

version: 2

sources:
  - name: raw__{{database_name | lower}}
    description: '[[ doc("{{database_name ~ '_description' }}") ]]'
    database: raw
    schema:  {{database_name}}
    loader:  Manual

    tables:
   {%- endset -%}
   {% do sources_yaml.append(header | string | replace('(*', '{%') | replace('*)', '%}') | replace('[[', '{{') | replace(']]', '}}') ) %}

    {% set query %}
    	select  source_table_name, stage_table_name, source_column_name, stage_column_description, stage_column_name, lower(stage_column_type) stage_column_type
        from internal.data_dictionary 
        where 
            database_name='{{database_name}}' and version_name='{{version_name}}' 
            {{ apply_filter }}
	    order by stage_table_name, column_order
    {% endset %}
    {% set rowset=run_query(query) %}

    {% set ns = namespace(last_table_name = 'NOT SET') %}
    {% for col in rowset %}
        {% set current_table_name = col.STAGE_TABLE_NAME | string %}
        {% if ns.last_table_name  != col.STAGE_TABLE_NAME | string %}
            {% if is_external %}
                {% do sources_yaml.append('      - name: "' ~  col.SOURCE_TABLE_NAME | lower ~ '"') %}
            {% else %}
                {% do sources_yaml.append('      - name: ' ~ col.STAGE_TABLE_NAME | lower) %}
            {% endif %}            
            {% if is_external %}
                {% do sources_yaml.append('        description: "' ~ col.STAGE_COLUMN_DESCRIPTION ~ '"' )  %}
            {% else %}
                {% do sources_yaml.append('        description: \'{{ doc("' ~ database_name ~ '_' ~ col.STAGE_TABLE_NAME ~ '_source_description' ~ '") }}\'' )  %}
            {% endif %}            
            {% do sources_yaml.append('        columns:') %}
            {% set ns.last_table_name = col.STAGE_TABLE_NAME | string %}
        {%endif %}

        {% if is_external %}
            {% set column_name = col.SOURCE_COLUMN_NAME %}
        {% else %}
            {% set column_name = col.STAGE_COLUMN_NAME %}
        {% endif %}
        {% do sources_yaml.append('          - name: ' ~ column_name) %}
        {% do sources_yaml.append('            data_type: ' ~ col.STAGE_COLUMN_TYPE ) %}
        {% do sources_yaml.append('            quote: true' ) %}
        {% if is_external %}
            {% do sources_yaml.append('            description: "' ~ col.STAGE_COLUMN_DESCRIPTION ~ '"' ) %}
        {% else %}
            {% do sources_yaml.append('            description: \'{{ doc("' ~ database_name ~ '_' ~ col.STAGE_TABLE_NAME ~ '_' ~ col.STAGE_COLUMN_NAME ~ '_source_description' ~ '") }}\'' ) %}
        {% endif %}            
        {% do sources_yaml.append('') %}
    {% endfor %}

    {% set joined = sources_yaml | join ('\n') %}
    {{ log(joined, info=True) }}
    {% do return(joined) %}

{% endmacro %}
