
{% macro check_all_tables_null(schema_name) %}
   {% set temp=[] %}
   
   {% set header %}
   {%- endset -%}
   {% do temp.append(header | string ) %}
        
   {% set footer %}
   {%- endset -%}  
   
   {% set template %}
       Select '{@table_name}' as table_name, '{@column_name}' as column_name, count(*) row_count, count({@column_name}) not_null_count, count(*) - count({@column_name}) null_count, max(as_of_dt) max_date, min(as_of_dt) as min_date from bkmrtg.tb_ldf_{@table_name}
   {% endset %}
   
    {%- set query -%}
	select  
		DISTINCT stage_table_name as table_name, stage_column_name as column_name
	from internal.data_dictionary 
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
	group by stage_table_name, stage_column_name
	order by stage_table_name, stage_column_name
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
   	{%- if not loop.first %}
		{% do temp.append('UNION ALL' | string ) %}
	{% endif %}
      {% do temp.append(template | string | replace('{@table_name}', tbl.TABLE_NAME) | replace('{@column_name}', tbl.COLUMN_NAME) ) %}
   {% endfor %}

   {% do temp.append(footer | string ) %}
   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
