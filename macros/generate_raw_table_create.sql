{% macro generate_raw_table_create(database_name='default', version_name='default', use_source=true, table_name='') %}

    {% if database_name=='default' %}
        {% set database_name = var('dictionary_database', target.database) %}
    {% endif %}
    {% if version_name=='default' %}
        {% set version_name = var('dictionary_database_version', 'default') %}
    {% endif %}

   {% set task_template %}
create or replace table raw.{{database_name}}.{@source_table_name} 
(
{@column_list_with_type}
);
    {% endset %}

    {%- set query -%}
  	select  
		distinct source_table_name
         , listagg(CONCAT('"',source_column_name,'" ',source_column_type), ',\n') within group ( order by column_order) as source_column_list
         , listagg(CONCAT(stage_column_name,' ',stage_column_type), ',\n') within group ( order by column_order) as stage_column_list
	from {{ref('data_dictionary') }} 
	where 
		database_name='{{database_name}}' and version_name='{{version_name}}'  and source_table_name='{{table_name}}'
	group by source_table_name
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
      {% set column_list = tbl.STAGE_COLUMN_LIST %}
      {% if use_source %}
         {% set column_list = tbl.SOURCE_COLUMN_LIST %}
      {% endif %}
      {% do print('\t' ~ task_template | string | replace('{@source_table_name}', tbl.SOURCE_TABLE_NAME) | replace('{@column_list_with_type}', column_list) ) %}
   {% endfor %}
	    
{% endmacro %}
