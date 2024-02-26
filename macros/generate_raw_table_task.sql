-- This script has not been ported to this new framework yet. I added to the project because it will eventually be needed
-- but currently the sources are updated quarterly and there is no automatic updating of the source data.

{% macro generate_raw_table_task(database_name, version_name, table_name) %}
   {% set temp=[] %}

   {% set header %}

   create file format if not exists raw.{{ database_name }}.{{ database_name }}_file_format
      type = csv
      field_delimiter = '|'
      null_if = ('NULL', 'null')
      empty_field_as_null = true
      date_format = 'MMYYYY'
      compression = gzip;
   ;
   
   create stage if not exists raw.{{ database_name }}.{{ database_name }}_stage
      storage_integration = snowflake_s3_integration
      url = 's3://iv-raw/fannie-sflpd/2022Q2'
      file_format = raw.{{ database_name }}.{{ database_name }}_file_format;

   {%- endset -%}
   {% do temp.append(header | string ) %}

  {% set template %}
   alter task if exists {{ target.database }}_reference_{@stage_table_name}_truncate suspend;
	create or replace task {{ target.database }}_reference_{@stage_table_name}_truncate
		ALLOW_OVERLAPPING_EXECUTION=FALSE
		WAREHOUSE=INGESTION_WH
		schedule='{{ var('dictionary_load_start') }}'
    AS 
      truncate table {{ target.database }}.reference.{@stage_table_name};

   alter task if exists {{ target.database }}_reference_{@stage_table_name}_reload suspend;
   create or replace task {{ target.database }}_reference_{@stage_table_name}_reload
		WAREHOUSE=INGESTION_WH
		AFTER {{ target.database }}_reference_{@stage_table_name}_truncate
   AS
      COPY INTO {{ target.database }}.reference.{@stage_table_name} from @bde.raw.data_lake_stage_toplevel/generic-lake/{@lower_import_file}
      file_format = (format_name= {{ target.database }}.reference.fayfin_csv_format, encoding='{@encoding}');

  alter task if exists {{ target.database }}_reference_{@stage_table_name}_reload resume;
  alter task if exists {{ target.database }}_reference_{@stage_table_name}_truncate resume;
    {% endset %}

    {%- set query -%}
  	select  
		DISTINCT stage_table_name, source_table_name
         -- this case statement was to handle inconsistent encoding which has been fixed
         ,CASE WHEN lower(stage_table_name)='demandcount' then 'iso-8859-1' 
           WHEN lower(stage_table_name)='transaction_codes' then 'utf-16' 
           WHEN lower(stage_table_name)='tbl_demand_status_temp' then 'utf-16' 
           WHEN lower(stage_table_name)='dim_hr_person_hist' then 'utf-16' 
           WHEN lower(stage_table_name)='payment_transaction_code' then 'utf-16' 
           WHEN lower(stage_table_name)='dim_investorportfolio_curr' then 'utf-16' 
           WHEN lower(stage_table_name)='investor_legal_entity' then 'utf-16' 
           WHEN lower(stage_table_name)='mail_address' then 'utf-16' 
           WHEN lower(stage_table_name)='mail_addressee' then 'utf-16' 
           WHEN lower(stage_table_name)='mail_attn_to' then 'utf-16' 
           WHEN lower(stage_table_name)='amq_loan' then 'utf-16' 
           WHEN lower(stage_table_name)='itemview_datainputs' then 'utf-16' 

         else IFF(startswith(stage_table_name,'demand'),'utf-16','iso-8859-1') END as encoding1
         , 'utf-16' as encoding
         , listagg(CONCAT(stage_column_name,' ',stage_column_type,'\n'), ',') within group ( order by column_order) as column_list, import_file
	from {{ ref('data_dictionary') }}
	where 
		database_name='{{database_name}}' and version_name='{{version_name}}'  and source_table_name='{{table_name}}'
	group by stage_table_name, source_table_name, import_file
	order by stage_table_name
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
      {% do temp.append(template | string | replace('{@stage_table_name}', tbl.STAGE_TABLE_NAME)| replace('{@encoding}', tbl.ENCODING) | replace('{@column_list_with_type}', tbl.COLUMN_LIST)  | replace('{@lower_import_file}', tbl.IMPORT_FILE | lower) ) %}
   {% endfor %}

   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
