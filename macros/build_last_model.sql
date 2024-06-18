{% macro build_last_model(primary_key, model_name) -%}

{%- set query1 -%}
	select  
		distinct database_name, source_table_name, a.stage_table_name, IFNULL(b.primary_key_list,'') primary_list, 
	from {{ ref('data_dictionary') }} a
	left join (
		Select stage_table_name, listagg(stage_column_name,',') within group (order by primary_key_order asc) as primary_key_list 
		from {{ ref('data_dictionary') }} 
		where primary_key_order is not null and stage_table_name='{{model_name}}' 
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

{% do print ('Select a.* from {{ source("loanserv_static", ' ~ model_name ~ ') }} a') %}
{% do print ('qualify row_number() over (partition by ' ~ primary_key ~ ' order by ' ~ primary_key_list | replace(primary_key + ',', '') ~ ' desc) = 1') %}

{%- endmacro %}
