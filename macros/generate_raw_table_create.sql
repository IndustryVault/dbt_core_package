{% macro generate_raw_table_create(database_name, version_name, table_name, use_source=true) %}

{% set header %}
use role load_role;
{%- endset -%}
   {% do print(header | string ) %}

   {% set task_template %}
create or replace table raw.{{database_name}}.{@source_table_name} 
(
{@column_list_with_type}
);
    {% endset %}

    {%- set query -%}
  	select  
		distinct source_table_name
         , listagg(CONCAT('"',source_column_name,'" ',source_column_type), ',') within group ( order by column_order) as source_column_list
         , listagg(CONCAT(stage_column_name,' ',stage_column_type), ',') within group ( order by column_order) as stage_column_list
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
      {% do print(task_template | string | replace('{@source_table_name}', tbl.SOURCE_TABLE_NAME) | replace('{@column_list_with_type}', column_list) ) %}
   {% endfor %}
	    
{% endmacro %}
