{% macro generate_doc_blocks(database_name, version_name, table_name, is_source_or_stage='Source') %}
   {% set temp=[] %}
   {% set header %}
(* comment *)

   The following section of doc blocks was generated using the macro generate_doc_blocks. Editing could be wiped out when this block is replaced.

   The block is represented by the 'begin generation' comment and completed by 'end generation'

   [BEGIN GENERATION]
(* endcomment *)
   {%- endset -%}
   {% do temp.append(header | string | replace('(*', '{%') | replace('*)', '%}') ) %}

   {% set template %}
(* docs {@doc_blockname}_description *)
{@description}
(* enddocs *)
    {% endset %}

    {%- set query -%}
  	select  
		source_table_name, stage_column_name, stage_column_description
	from raw.internal.data_dictionary 
	where 
		database_name='{{database_name}}' and version_name='{{version_name}}'  and source_table_name='{{table_name}}'
	order by stage_table_name, stage_column_name, column_order
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {%- for tbl in tables -%}
      {%- do temp.append(template | string | replace('{@doc_blockname}',  database_name ~ '_' ~ table_name ~ '_' ~ tbl.STAGE_COLUMN_NAME ~ '_' ~ is_source_or_stage ) | replace('{@description}', tbl.STAGE_COLUMN_DESCRIPTION) | replace('(*', '{%') | replace('*)', '%}') ) -%}
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
