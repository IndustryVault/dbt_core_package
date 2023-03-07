{% macro generate_raw_table_load(database_name, version_name, table_name, stage_url) %}
   {% set temp=[] %}

   {% set header %}
   use role accountadmin;
   create file format if not exists raw.{{ database_name }}.{{ database_name }}_{{ table_name }}_file_format
      type = csv
      field_delimiter = '|'
      null_if = ('NULL', 'null')
      empty_field_as_null = true
      date_format = 'MMYYYY'
      compression = gzip
   ;
   
   create stage if not exists raw.{{ database_name }}.{{ database_name }}_{{table_name }}_stage
      storage_integration = snowflake_s3_integration
      url = '{{stage_url}}'
      file_format = raw.{{ database_name }}.{{ database_name }}_{{table_name}}_file_format;

   copy into raw.{{database_name }}.{{table_name}} from @raw.{{ database_name }}.{{ database_name }}_{{table_name}}_stage;
   {%- endset -%}
   {% do temp.append(header | string ) %}

   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
