{% macro build_last_model(primary_key, model_name) -%}

{%- set query1 -%}
	select  
		distinct database_name, source_table_name, a.stage_table_name, IFNULL(b.primary_key_list,'') primary_list, 
	from {{ ref('primary_keys') }} a
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
