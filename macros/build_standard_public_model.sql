e{% macro build_standard_public_model(model_name) -%}

{%- set query1 -%}
	select  
		distinct database_name, source_table_name, a.stage_table_name, IFNULL(b.primary_key_list,'') primary_list
	from {{ ref('data_dictionary') }} a
	left join (
		Select stage_table_name, listagg(stage_column_name,',') within group (order by primary_key_order asc) as primary_key_list 
		from {{ ref('data_dictionary') }} 
		where primary_key_order is not null 
		group by stage_table_name
	) b on a.stage_table_name=b.stage_table_name
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
		and a.stage_table_name='{{model_name}}' 
		and a.stage_column_name is not null
        and is_public = 1
{%- endset -%}

{% if execute %}
    {%- set raw_database_name  = 'raw__' ~ run_query(query1)[0][0] %}  
    {%- set source_table = run_query(query1)[0][1] %}
    {%- set primary_key_list = run_query(query1)[0][3] %}  
{% else %}
    {%- set source_table = '' %}  
    {%- set primary_key_list = '' %}  
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
	{% do print( '\tfrom {{ source("' ~ raw_database_name ~'","' ~ source_table ~ '") }}') %}
{% endif %}

{% do print(')\n\nselect \n\t *\nfrom filtered') %}
{% if execute %}
	{% if primary_key_list != '' %}
		{% do print('{% if var(\'enable_force_uniqueness\') == \'true\' %}') %}
		{% do print('qualify row_number() over (partition by ' ~ primary_key_list ~' order by null) = 1') %}
		{% do print('{% endif %}') %}
	{% endif %}
{% endif %}

{%- endmacro %}
