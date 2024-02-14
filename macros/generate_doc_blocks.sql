{% macro generate_doc_blocks(database_name, version_name, table_name='ALL', is_source_or_stage='source') %}
   {% set temp=[] %}
{% set not_provided = database_name ~ '_' ~ is_source_or_stage %}
   {% set header %}
(* comment *)

   The following section of doc blocks was generated using the macro generate_doc_blocks. Editing could be wiped out when this block is replaced.

   The block is represented by the 'begin generation' comment and completed by 'end generation'

   [BEGIN GENERATION]

(* docs {@doc_blockname}_description *)
{@description}
(* enddocs *)
(* docs {@doc_blockname}_description_not_provided *)
Not Provided
(* enddocs *)
(* endcomment *)
   {%- endset -%}
   
   {%- do temp.append(header | string | replace('{@doc_blockname}',  not_provided) | replace('{@description}', 'Not Provided') | replace('(*', '{%') | replace('*)', '%}') ) -%}

   {% set template %}
(* docs {@doc_blockname}_description *)
{@description}
(* enddocs *)
    {% endset %}
    {%- set query -%}
        select  distinct 
            CASE 
                WHEN '{{is_source_or_stage}}'='source' then source_table_name
                WHEN '{{is_source_or_stage}}'='stage' then stage_table_name
                ELSE '****'
            end as table_name,
	    source_table_name
        from internal.data_dictionary 
        where 
            database_name='{{database_name}}' and version_name='{{version_name}}'  and (source_table_name='{{table_name}}' OR '{{table_name}}'='ALL')
        order by 1
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {%- for tbl in tables -%}
      {%- do temp.append(template | string | replace('{@doc_blockname}',  database_name ~ '_' ~ tbl.TABLE_NAME ~ '_' ~ is_source_or_stage) | replace('{@description}', 'Not Provided') | replace('(*', '{%') | replace('*)', '%}') ) -%}
   {%- endfor -%}
    {%- set query -%}
  	select  
		source_table_name, stage_table_name, stage_column_name, stage_column_description,
	     CASE 
                WHEN '{{is_source_or_stage}}'='source' then source_column_description
                WHEN '{{is_source_or_stage}}'='stage' then stage_column_description
                ELSE '****'
            end as column_description,
	   CASE 
                WHEN '{{is_source_or_stage}}'='source' then source_table_name
                WHEN '{{is_source_or_stage}}'='stage' then stage_table_name
                ELSE '****'
            end as table_name,
         CASE 
                WHEN '{{is_source_or_stage}}'='source' then source_column_name
                WHEN '{{is_source_or_stage}}'='stage' then stage_column_name
                ELSE '****'
            end as column_name
	from {{ref('data_dictionary')}} 
	where 
		database_name='{{database_name}}' and version_name='{{version_name}}'  and (table_name='{{table_name}}' OR '{{table_name}}'='ALL')
	and ('{{is_source_or_stage}}'='source' OR is_public)
    where 
	order by stage_table_name, stage_column_name, column_order
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {%- for tbl in tables -%}
      {%- do temp.append(template | string | replace('{@doc_blockname}',  database_name ~ '_' ~ tbl.TABLE_NAME ~ '_' ~ tbl.COLUMN_NAME ~ '_' ~ is_source_or_stage) | replace('{@description}', tbl.COLUMN_DESCRIPTION) | replace('(*', '{%') | replace('*)', '%}') ) -%}
   {%- endfor -%}
  
     {% set footer %}
(* comment *)
   [END GENERATION]
(* endcomment *)
   {%- endset -%}
   {% do temp.append(footer | string | replace('(*', '{%') | replace('*)', '%}') ) %}
   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
