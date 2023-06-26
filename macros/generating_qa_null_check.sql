
{% macro check_all_tables_null(schema_name) %}
   {% set temp=[] %}
   
   {% set header %}
   {%- endset -%}
   {% do temp.append(header | string ) %}
        
   {% set footer %}
   {%- endset -%}  
   
   {% set template %}
       Select '{@table_name}' as table_name, '{@column_name}' as column_name, '{@description}' as description, count(*) row_count, count({@column_name}) not_null_count
       , count(*) - count({@column_name}) null_count, max(etl_recorded_GMTS) max_date, min(etl_recorded_GMTS) as min_date from bnkns.{@table_name}
   {% endset %}
	
   {% set template2 %}
       Select '{@table_name}' as table_name, '{@column_name}' as column_name, '{@description}' as description, count(*) row_count, count({@column_name}) not_null_count
       , count(*) - count({@column_name}) null_count, max(as_of_dt) max_date, min(as_of_dt) as min_date from bkmrtg.tb_{@table_name}
   {% endset %}
	{%- set query -%}
	with mapping as
(
    select replace(upper(boddh_table_name),'TB_','') as boddh_table_name
        ,boddh_column_name,SNOWFLAKE_TABLE_NAME, SNOWFLAKE_COLUMN_NAME, transformations
    from public.boddh_to_snowflake b2s
)
        select DISTINCT 
            SNOWFLAKE_TABLE_NAME as table_name, sNOWFLAKE_COLUMN_NAME as column_name
         , REPLACE(source_column_description, '''','') as description
         , transformations

	from internal.data_dictionary 
    inner join mapping on mapping.boddh_table_name=upper(source_table_name) and boddh_column_name=source_column_name
	where 
		 LEFT(upper(source_column_name),8) != 'PROCESS_'
	{%- endset -%}
	
    {%- set query2 -%}
        select DISTINCT 
		CASE WHEN startswith(upper(source_table_name), 'LDF_MONTHLY') THEN CONCAT(REPLACE(upper(source_table_name), '_MONTHLY',''),'_MONTHLY') 
			ELSE source_table_name 
			end as temp_table_name
		, CASE WHEN contains(upper(temp_table_name), 'DEED_IN_LIEU') THEN REPLACE(upper(temp_table_name), 'DEED_IN_LIEU','DIL') ELSE temp_table_name END as table_name
		, source_column_name as column_name, REPLACE(source_column_description, '''','') as description
	from internal.data_dictionary 
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
		and LEFT(upper(source_column_name),8) != 'PROCESS_'
	group by temp_table_name, table_name, source_column_name, source_column_description
	order by temp_table_name, table_name, source_column_name, description
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
