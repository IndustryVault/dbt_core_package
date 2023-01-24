{% macro generate_from_dictionary_source_yml(database_name='default', version_name='default', is_external='false', apply_filter='') %}

    {% set sources_yaml=[] %}

    {% if database_name=='default' %}
        {% set database_name = var('dictionary_database', target.database) %}
    {% endif %}
    {% if version_name=='default' %}
        {% set version_name = var('dictionary_database_version', 'default') %}
    {% endif %}

    {% if is_external == 'false' %}
        {% set source_name = 'raw__' ~ database_name | lower %}
        {% set source_description = '[[ doc("{{database_name ~ '_source_description' }}") ]]' %}
        {% set source_database = 'raw' %}
        {% set source_schema = database_name %}
    {% else %}
        {% set source_name = database_name | lower %}
        {% set source_description = 'None Provided' %}
        {% set source_database = database_name %}
        {% set source_schema = 'public' %}
    {% endif %}

    {% set header %}
#
#   This file was originally generated by the macro generate_from_dictionary_source_yml and is now under manual control. Replacing with a regenerated file will lose changes!!
#

version: 2

sources:
  - name: {{source_name}}
    description: {{source_description}}
    database: {{source_database}}
    schema:  {{database_schema}}
    loader:  Manual

    tables:
   {%- endset -%}
   {% do sources_yaml.append(header | string | replace('(*', '{%') | replace('*)', '%}') | replace('[[', '{{') | replace(']]', '}}') ) %}

    {% set query %}
    	select  
            source_table_name, stage_table_name, source_column_name, stage_column_description
            , stage_column_name, source_column_type, lower(stage_column_type) stage_column_type, allow_null
	    , CASE WHEN '{{is_external}}' = 'false' then source_table_name else stage_table_name end as table_name
	    , CASE WHEN '{{is_external}}' = 'false' then source_column_name else stage_column_name end as column_name
	    , CASE WHEN '{{is_external}}' = 'false' then source_column_type else stage_column_type end as column_type
	    , CASE WHEN '{{is_external}}' = 'false' then '_source_description' else '_stage_description' end as suffix
	    
	    from internal.data_dictionary 
        where 
            database_name='{{database_name}}' and version_name='{{version_name}}' 
            {{ apply_filter }}
	    order by stage_table_name, column_order
    {% endset %}
    {% set rowset=run_query(query) %}

    {% set ns = namespace(last_table_name = 'NOT SET') %}
    {% for col in rowset %}
        {% set current_table_name = col.TABLE_NAME | string %}
        {% if ns.last_table_name  != col.TABLE_NAME | string %}
            {% do sources_yaml.append('      - name: "' ~  col.TABLE_NAME ~ '"') %}
            {% do sources_yaml.append('        description: \'{{ doc("' ~ database_name ~ '_' ~ col.TABLE_NAME ~ col.suffix ~ '") }}\'' )  %}          
            {% do sources_yaml.append('        columns:') %}
            {% set ns.last_table_name = col.TABLE_NAME | string %}
        {%endif %}

        {% do sources_yaml.append('          - name: "' ~ col.COLUMN_NAME ~ '"') %}
        {% do sources_yaml.append('            data_type: ' ~ col.SOURCE_COLUMN_TYPE ) %}
        {% do sources_yaml.append('            quote: true' ) %}
        {% do sources_yaml.append('            description: \'{{ doc("' ~ database_name ~ '_' ~ col.TABLE_NAME ~ '_' ~ col.COLUMN_NAME ~ col.suffix ~ '") }}\'' ) %}
        
        {% if col.ALLOWS_NULL == false %}
            {% do sources_yaml.append('            tests: ' ) %}
            {% do sources_yaml.append('              - not null' ) %}
        {% endif %}
        {% do sources_yaml.append('') %}
    {% endfor %}

    {% set joined = sources_yaml | join ('\n') %}
    {{ log(joined, info=True) }}
    {% do return(joined) %}

{% endmacro %}
