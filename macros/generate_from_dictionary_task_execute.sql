{% macro generate_from_dictionary_task_execute(dictionary_name='data_dictionary', target_schema='external', include_truncate='false', include_import_file='true') %}

   {% set header %}
   use database {{ target.database }};
   use schema {{ target_schema }};
   {%- endset -%}
   {% do print(header | string ) %}

   {% if include_truncate == 'true' %}
   {% set template %}
      execute task {{ target.database }}_{{ target_schema }}_{@stage_table_name}_truncate ;
   {% endset %}
   {% else %}
   {% set template %}
      execute task {{ target.database }}_{{ target_schema }}_{@stage_table_name}_refresh ;
   {% endset %}   {% endif %}

   {% if include_import_file == 'true' %}
    {%- set query -%}
  	select  
		DISTINCT stage_table_name, source_table_name
         , 'utf-16'  as encoding
	from internal.{{dictionary_name}}
	where 
		database_name='{{var('dictionary_database', target.database)}}' and version_name='{{var('dictionary_database_version')}}' 
	group by stage_table_name, source_table_name, import_file
	order by stage_table_name
   {%- endset -%}
   {%- else -%}
    {%- set query -%}
  	select  
		DISTINCT stage_table_name, source_table_name
         , 'utf-16'  as encoding
	from internal.{{dictionary_name}}
	where 
		database_name='{{var('dictionary_database', target.database)}}' and version_name='{{var('dictionary_database_version')}}' 
	group by stage_table_name, source_table_name
	order by stage_table_name
   {%- endset -%}
   {%- endif %}
   
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
      {% do print(template | string | replace('{@source_table_name}', tbl.SOURCE_TABLE_NAME) | replace('{@stage_table_name}', tbl.STAGE_TABLE_NAME)) %}
   {% endfor %}

{% endmacro %}
--
