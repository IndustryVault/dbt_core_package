{% macro build_last_model(primary_key, model_name) -%}

{%- set query1 -%}
	select  
		distinct IFNULL(primary_key_list,'') primary_list, 
	from {{ ref('primary_keys') }}
	where stage_table_name='{{model_name}}'
{%- endset -%}

{% if execute %}
    {%- set primary_key_list = run_query(query1)[0][0] %}  
{% else %}
    {%- set primary_key_list = '' %}  
{% endif %}

{% do print ('Select a.* from {{ source("loanserv_static", ' ~ model_name ~ ') }} a') %}
{% do print ('qualify row_number() over (partition by ' ~ primary_key ~ ' order by ' ~ primary_key_list | replace(primary_key + ',', '') ~ ' desc) = 1') %}

{%- endmacro %}
