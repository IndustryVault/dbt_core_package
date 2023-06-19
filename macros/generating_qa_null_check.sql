
{% macro check_all_tables_null(schema_name) %}
   {% set temp=[] %}
   
   {% set header %}
   {%- endset -%}
   {% do temp.append(header | string ) %}
        
   {% set footer %}
   {%- endset -%}  
   
   {% set template %}
       Select '{@table_name}' as table_name, '{@column_name}' as column_name, '{@description}' as description, count(*) row_count, count({@column_name}) not_null_count
       , count(*) - count({@column_name}) null_count, max(as_of_dt) max_date, min(as_of_dt) as min_date from bkmrtg.tb_{@table_name}
   {% endset %}
   
    {%- set query -%}
    	select
    		DISTINCT CASE WHEN startswith(upper(source_table_name), 'LDF_MONTHLY') THEN CONCAT(REPLACE(upper(source_table_name), '_MONTHLY',''),'_MONTHLY') ELSE source_table_name end as table_name, source_column_name as column_name, REPLACE(source_column_description, '''','') as description
	from internal.data_dictionary 
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
	group by table_name, source_column_name, source_column_description
	order by table_name, source_column_name, description
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
   	{%- if not loop.first %}
		{% do temp.append('UNION ALL' | string ) %}
	{% endif %}
      {% do temp.append(template | string | replace('{@table_name}', tbl.TABLE_NAME) | replace('{@column_name}', tbl.COLUMN_NAME) | replace('{@description}', tbl.DESCRIPTION) ) %}
   {% endfor %}

   {% do temp.append(footer | string ) %}
   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
