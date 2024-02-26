{% macro generate_doc_blocks_tables(database_name, version_name, table_name='ALL', is_source_or_stage='source') %}

{% set header %}
(* comment *)

  The following section of doc blocks was generated using the macro generate_doc_blocks_tables. Which standardizes the names of the doc blocks but does not have any description information.
  Users should overwrite the 'Not Provided' descriptions as needed. If new tables are added to the source then manually update this file. DO NOT REGENERATE as you will overwrite manual
  descriptions.

   The block is represented by the 'begin generation' comment and completed by 'end generation'

   [BEGIN GENERATION]

(* endcomment *)

(* docs {@doc_blockname}_description *)
Not Provided
(* enddocs *)
{%- endset -%} 
   
{%- do print(header | string | replace('{@doc_blockname}',  database_name ~ '_' ~ is_source_or_stage)  | replace('(*', '{%') | replace('*)', '%}') ) -%}

{% set template %}
(* docs {@doc_blockname}_description *)
Not Provided
(* enddocs *)
{% endset %}
	
    {%- set query -%}
        select  distinct 
            CASE 
                WHEN '{{is_source_or_stage}}'='source' then lower(source_table_name)
                WHEN '{{is_source_or_stage}}'='stage' then lower(stage_table_name)
                ELSE '****'
            end as table_name
        from internal.data_dictionary 
        where 
            database_name='{{database_name}}' and version_name='{{version_name}}'  and (table_name='{{table_name}}' OR '{{table_name}}'='ALL')
        order by 1
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {%- for tbl in tables -%}
      {%- do print(template | string | replace('{@doc_blockname}',  database_name ~ '_' ~ tbl.TABLE_NAME ~ '_' ~ is_source_or_stage) | replace('(*', '{%') | replace('*)', '%}') ) -%}
   {%- endfor -%}

     {% set footer %}
(* comment *)
   [END GENERATION]
(* endcomment *)
   {%- endset -%}
   {% do print(footer | string | replace('(*', '{%') | replace('*)', '%}') ) %}

{% endmacro %}
