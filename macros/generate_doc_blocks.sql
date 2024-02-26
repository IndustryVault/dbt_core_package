{% macro generate_doc_blocks(database_name='default', version_name='default', table_name='ALL', is_source_or_stage='source') %}
     {% if database_name=='default' %}
        {% set database_name = var('dictionary_database', target.database) %}
    {% endif %}
    {% if version_name=='default' %}
        {% set version_name = var('dictionary_database_version', 'default') %}
    {% endif %}
	
{% set header %}
(* comment *)

   The following section of doc blocks was generated using the macro generate_doc_blocks. All description values come from the data_dictionary model and should not be edited
	here under any circumstances. Update the source material to change the data dictionary and then re-generate this file.

   The block is represented by the 'begin generation' comment and completed by 'end generation'

   [BEGIN GENERATION]

(* endcomment *)
   {%- endset -%}
   
   {%- do print(header | string | replace('(*', '{%') | replace('*)', '%}') ) -%}

{% set template %}
(* docs {@doc_blockname}_description *)
{@description}
(* enddocs *)
{% endset %}

	{%- set query -%}
  	select  
	   CASE 
                WHEN '{{is_source_or_stage}}'='source' then source_table_name
                WHEN '{{is_source_or_stage}}'='stage' then stage_table_name
                ELSE '****'
            end as table_name,
         CASE 
                WHEN '{{is_source_or_stage}}'='source' then source_column_name
                WHEN '{{is_source_or_stage}}'='stage' then stage_column_name
                ELSE '****'
            end as column_name,
	CASE 
                WHEN '{{is_source_or_stage}}'='source' then source_column_description
                WHEN '{{is_source_or_stage}}'='stage' then stage_column_description
                ELSE '****'
            end as column_description

	from {{ref('data_dictionary')}} 
	where 
		database_name='{{database_name}}' and version_name='{{version_name}}'  and (table_name='{{table_name}}' OR '{{table_name}}'='ALL')
	and ('{{is_source_or_stage}}'='source' OR is_public)
	and column_description is not null
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {%- for tbl in tables -%}
      {%- do print(template | string | replace('{@doc_blockname}',  database_name ~ '_' ~ tbl.TABLE_NAME ~ '_' ~ tbl.COLUMN_NAME ~ '_' ~ is_source_or_stage) | replace('{@description}', tbl.COLUMN_DESCRIPTION) | replace('(*', '{%') | replace('*)', '%}') ) -%}
   {%- endfor -%}
  
     {% set footer %}
(* comment *)
   [END GENERATION]
(* endcomment *)
   {%- endset -%}
   {% do print(footer | string | replace('(*', '{%') | replace('*)', '%}') ) %}
{% endmacro %}
