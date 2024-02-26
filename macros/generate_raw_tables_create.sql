
{% macro generate_from_dictionary_raw_tables_create (database_name='default', version_name='default', description_method='none', apply_filter='',add_lightdash=false) %}

    {% if database_name=='default' %}
        {% set database_name = var('dictionary_database', target.database) %}
    {% endif %}
    {% if version_name=='default' %}
        {% set version_name = var('dictionary_database_version', 'default') %}
    {% endif %}

{%- set query -%}
	
    select 
		distinct source_table_name  
	from {{ref('data_dictionary') }}
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
--		and stage_column_name is not null
--        and is_public = 1
    order by source_table_name
{%- endset -%}

{%- set rowset = run_query(query) %}    
{% for item in rowset %} 
    {{ iv_common.generate_raw_table_create(
        table_name=item.SOURCE_TABLE_NAME
    ) }}

{% endfor %} 
{%endmacro%}
