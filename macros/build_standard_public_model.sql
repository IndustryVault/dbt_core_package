e{% macro build_standard_public_model(model_name) -%}

{%- set query1 -%}
	select  
		distinct database_name, source_table_name, stage_table_name
	from {{ ref('data_dictionary') }}
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
		and stage_table_name='{{model_name}}' 
		and stage_column_name is not null
        and is_public = 1
{%- endset -%}

{% if execute %}
    {%- set raw_database_name  = 'raw__' ~ run_query(query1)[0][0] %}  
    {%- set source_table = run_query(query1)[0][1] %}  
{% else %}
    {%- set source_table = '' %}  
{% endif %}

{%- set query -%}
	select  
		stage_column_name, stage_column_type, source_column_name  
	from {{ ref('data_dictionary') }}
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
		and stage_table_name='{{model_name}}' 
		and stage_column_name is not null
        and is_public = 1
	order by stage_column_name
{%- endset -%}
{% do print('\n') %}
{% do print ('WITH filtered as ( ') %}
{% do print ('\tselect  ') %}
        {%- set columns = run_query(query) %}    
        {% for column in columns %}
		{%- if not loop.first -%}{%- set prefix = '\t\t,' -%}{%- else -%}{% set prefix = '\t\t' -%}{%- endif -%}
        	{%- do print(prefix ~ '"' ~ column.SOURCE_COLUMN_NAME ~ '"::' ~ column.STAGE_COLUMN_TYPE ~ ' AS ' ~ column.STAGE_COLUMN_NAME) %}
        {% endfor %}
{% if execute %}
	{% do print( '\n\tfrom {{ source("' ~ raw_database_name ~'","' ~ source_table ~ '") }}') %}
{% endif %}

{% do print(')\n\nselect \n\t *\nfrom filtered') %}

{%- endmacro %}
