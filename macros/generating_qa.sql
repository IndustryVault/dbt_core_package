
{% macro generate_all_tables_current(schema_name) %}
   {% set temp=[] %}
   
   {% set header %}
    Select table_name, max_date from (
   {%- endset -%}
   {% do temp.append(header | string ) %}
        
   {% set footer %}
    ) tbl
    where max_date != CURRENT_DATE
   {%- endset -%}  
   
   {% set template %}
       Select '{@table_name}' as table_name, max(as_of_date) max_date from {@schema_name}.{@table_name}
   {% endset %}
   
   {%- set query -%}
      select  table_name from information_schema.tables where lower(table_schema)='{@schema_name}'
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
     {%- if not loop.first %} UNION ALL {% endif %}
      {% do temp.append(template | string | replace('{@table_name}', tbl.TABLE_NAME)| replace('{@schema_name}', schema_name) ) %}
   {% endfor %}

   {% do temp.append(footer | string ) %}
   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
