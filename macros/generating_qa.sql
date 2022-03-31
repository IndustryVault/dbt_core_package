
{% macro check_all_tables_current(schema_name) %}
   {% set temp=[] %}
   
   {% set header %}
    Select table_name, max_date from (
   {%- endset -%}
   {% do temp.append(header | string ) %}
        
   {% set footer %}
    ) tbl
    where max_date != CURRENT_DATE
   {%- endset -%}  
   
   {% set template %}
       Select '{@table_name}' as table_name, max(as_of_date) max_date from portfolio.{@table_name}
   {% endset %}
   
    {%- set query -%}
	select  
		DISTINCT stage_table_name
	from internal.dictionary 
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
		and has_column_issue=0 and has_table_issue=0
	group by stage_table_name
	order by stage_table_name
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
     {%- if not loop.first %} UNION ALL {% endif %}
      {% do temp.append(template | string | replace('{@table_name}', tbl.TABLE_NAME) ) %}
   {% endfor %}

   {% do temp.append(footer | string ) %}
   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
