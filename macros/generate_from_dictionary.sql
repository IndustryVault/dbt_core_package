{# 
    marcos related to reading a file format from a dictionary and generating the scaffording of the dbt project. all references from the dictionary are by column name so the
    dictionary must match the same naming style.
#}

# Assumes that the external tables have been updated to include the most recent data.
# Uses an insert into to pull data through all layers in the stack and populate tables in
# the portfolio schema. Should generate the good performance for public usage as well as allowing
# for easy change.

{% macro generate_from_dictionary_create_tables(dictionary_name='dictionary', target_schema='input') %}
   {% set temp=[] %}
   {% set header %}
   use database {{ target.database }};
   {%- endset -%}
   {% do temp.append(header | string ) %}

   {% set task_template %}
CREATE OR REPLACE table {{ target.database }}.{{ target_schema }}.{@source_table_name} 
(
    {@column_list_with_type}
);
    {% endset %}

    {%- set query -%}
  	select  
		DISTINCT stage_table_name, source_table_name
         , listagg(CONCAT('"',source_column_name,'" ',source_column_type,'\n'), ',') within group ( order by column_order) as column_list, import_file
	from internal.{{dictionary_name}}
	where 
		database_name='{{var('dictionary_database', target.database)}}' and version_name='{{var('dictionary_database_version')}}' 
	group by stage_table_name, source_table_name, import_file
	order by stage_table_name
   {%- endset -%}
   {%- set tables = run_query(query)  -%}   
   
   {% for tbl in tables %}
      {% do temp.append(task_template | string | replace('{@source_table_name}', tbl.SOURCE_TABLE_NAME) | replace('{@column_list_with_type}', tbl.COLUMN_LIST)) %}
   {% endfor %}

   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
--
{% macro generate_from_dictionary_load_tables(dictionary_name='dictionary', target_schema='input', include_truncate='true') %}
   {% set temp=[] %}

   {% set header %}
   use database {{ target.database }};
   use schema {{ target_schema }};
   CREATE FILE FORMAT IF NOT EXISTS {{ target.database }}.{{ target_schema }}.fayfin_csv_format
   TYPE = 'CSV'
   field_delimiter = '|'
   skip_header = 1
   TRIM_SPACE = TRUE
   ESCAPE_UNENCLOSED_FIELD = NONE 
   NULL_IF='NA'
   record_delimiter='\r\n'
   ;
   {%- endset -%}
   {% do temp.append(header | string ) %}

   {% if include_truncate == 'true' %}
	  {% set template %}
	   alter task if exists {{ target.database }}_{@source_table_name}_truncate suspend;
		create or replace task {{ target.database }}_{@source_table_name}_truncate
			ALLOW_OVERLAPPING_EXECUTION=FALSE
			WAREHOUSE=INGESTION_WH
			schedule='{{ var('load_start') }}'
	    AS 
	      truncate table {{ target.database }}.{{ target_schema }}.{@source_table_name};

	   alter task if exists {{ target.database }}_{@source_table_name}_reload suspend;
	   create or replace task {{ target.database }}_{@source_table_name}_reload
			WAREHOUSE=INGESTION_WH
			AFTER {{ target.database }}_{@source_table_name}_truncate
	   AS
	      COPY INTO {{ target.database }}.{{ target_schema }}.{@source_table_name} from @bde.raw.data_lake_stage_toplevel/generic-lake/{@lower_import_file}
	      file_format = (format_name= {{ target.database }}.{{ target_schema }}.fayfin_csv_format, encoding='{@encoding}');

	  alter task if exists {{ target.database }}_{@source_table_name}_reload resume;
	  alter task if exists {{ target.database }}_{@source_table_name}_truncate resume;
	    {% endset %}
    {% else %}
       {% set template %}
	   alter task if exists {{ target.database }}_{@source_table_name}_reload suspend;
	   create or replace task {{ target.database }}_{@source_table_name}_reload
			WAREHOUSE=INGESTION_WH
			schedule='{{ var('load_start') }}'
	   AS
	      COPY INTO {{ target.database }}.{{ target_schema }}.{@source_table_name} from @bde.raw.data_lake_stage_toplevel/generic-lake/{@lower_import_file}
	      file_format = (format_name= {{ target.database }}.{{ target_schema }}.fayfin_csv_format, encoding='{@encoding}');

	  alter task if exists {{ target.database }}_{@source_table_name}_reload resume;
      {% endset %}
  {% endif %}

    {%- set query -%}
  	select  
		DISTINCT source_table_name
         , 'utf-16'  as encoding
         , listagg(CONCAT(stage_column_name,' ',stage_column_type,'\n'), ',') within group ( order by column_order) as column_list, import_file
	from internal.{{dictionary_name}}
	where 
		database_name='{{var('dictionary_database', target.database)}}' and version_name='{{var('dictionary_database_version')}}' 
	group by source_table_name, import_file
	order by source_table_name
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
      {% do temp.append(template | string | replace('{@source_table_name}', tbl.SOURCE_TABLE_NAME)| replace('{@encoding}', tbl.ENCODING) | replace('{@column_list_with_type}', tbl.COLUMN_LIST)  | replace('{@lower_import_file}', tbl.IMPORT_FILE | lower) ) %}
   {% endfor %}

   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
--
{% macro generate_from_dictionary_execute_tables(dictionary_name='dictionary', target_schema='input', include_truncate='true') %}
   {% set temp=[] %}

   {% set header %}
   use database {{ target.database }};
   use schema {{ target_schema }};
   {%- endset -%}
   {% do temp.append(header | string ) %}

   {% if include_truncate == 'true' %}
   {% set template %}
      execute task {{ target.database }}_{@source_table_name}_truncate ;
   {% endset %}
   {% else %}
   {% set template %}
      execute task {{ target.database }}_{@source_table_name}_reload ;
   {% endset %}   {% endif %}

    {%- set query -%}
  	select  
		DISTINCT stage_table_name, source_table_name
         , 'utf-16'  as encoding
	from internal.{{dictionary_name}}
	where 
		database_name='{{var('dictionary_database', target.database)}}' and version_name='{{var('dictionary_database_version')}}' 
	group by stage_table_name, source_table_name, import_file
	order by stage_table_name
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   
   {% for tbl in tables %}
      {% do temp.append(template | string | replace('{@source_table_name}', tbl.SOURCE_TABLE_NAME)) %}
   {% endfor %}

   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
--
{% macro generate_from_dictionary_incremental_load_tasks(include_tasks='true', use_standard_warehouse='true') %}
   {% set temp=[] %}
   
   {% set header %}
   /*
   // execute this command first then execute the lower commands that are based on the last_query_id.
	show tasks  like '{{ var('dictionary_database') }}_%';
	show tasks  like '{{ var('dictionary_database') }}_%_REFRESH';

	Select LISTAGG(REPLACE('DROP TASK IF EXISTS {@TASK_NAME};' || CHAR(13), '{@TASK_NAME}', CONCAT_WS('.',"database_name","schema_name","name")), '') as drop_task
	FROM TABLE(result_scan(last_query_id()));

	Select LISTAGG(REPLACE('ALTER TASK {@TASK_NAME} suspend;' || CHAR(13), '{@TASK_NAME}', CONCAT_WS('.',"database_name","schema_name","name")), '') as drop_task
	FROM TABLE(result_scan(last_query_id())) ;
	
	// This is a very slow query
	Select * From snowflake_account_usage.task_history where database={{ var('dictionary_database') | upper }} LIMIT 10;
   */
   use database {{ var('dictionary_database') }};
   use schema external;
   set schedule = '{{ var('dictionary_load_start') }}';
   {%- endset -%}

   {% do temp.append(header | string ) %}
   {% if include_tasks == 'true' %}
   {% set task_template %}

    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_refresh suspend;
    create or replace task {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_refresh
        ALLOW_OVERLAPPING_EXECUTION=FALSE
        WAREHOUSE={@warehouse}
	schedule=$schedule
    AS 
        alter external table external.{@source_table_name} refresh;

    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_historical_delete suspend;
    create or replace task {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_historical_delete
       AFTER {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_refresh
    AS 
        call external.task__delete_by_cycle_date('historical', '{@stage_table_name}', '{@source_table_name}');
    
    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_historical_load suspend;
    create or replace task {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_historical_load
      AFTER {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_historical_delete
    AS
        call external.task__insert_by_cycle_date('historical','{@stage_table_name}', '{@source_table_name}');

    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_portfolio_delete suspend;
    create or replace task {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_portfolio_delete
       AFTER {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_historical_load
    AS 
        call external.task__delete_by_as_of_date('portfolio', '{@stage_table_name}', '{@source_table_name}');

    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_portfolio_load suspend;
    create or replace task {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_portfolio_load
        AFTER {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_portfolio_delete
    AS
        call external.task__insert_by_as_of_date('portfolio','{@stage_table_name}', '{@source_table_name}');

    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_portfolio_load resume;
    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_portfolio_delete resume;
    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_historical_load resume;
    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_historical_delete resume;
    alter task if exists {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_refresh resume;
    {% endset %}
       {% else %}
    {% set task_template_old %}
    execute task {{ var('dictionary_database') }}.external.{{ var('dictionary_database') }}_external_{@stage_table_name}_refresh;
    {% endset %}      
     {% set task_template %}

    alter external table external.{@source_table_name} refresh;
    call external.task__delete_by_cycle_date('historical', '{@stage_table_name}', '{@source_table_name}');
    call external.task__insert_by_cycle_date('historical','{@stage_table_name}', '{@source_table_name}');
    call external.task__delete_by_as_of_date('portfolio', '{@stage_table_name}', '{@source_table_name}');
    call external.task__insert_by_as_of_date('portfolio','{@stage_table_name}', '{@source_table_name}');
    {% endset %}  
    {% endif %}

    {%- set query -%}
	select  
		DISTINCT stage_table_name, source_table_name, listagg(stage_column_name, ',') within group ( order by column_order) as column_list
	from internal.dictionary 
	where 
		database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
		and has_column_issue=0 and has_table_issue=0
	group by stage_table_name, source_table_name
	order by stage_table_name
   {%- endset -%}
   {%- set tables = run_query(query) -%}   
   {% if use_standard_warehouse == 'true' %}
   	{% set warehouse = 'INGESTION_WH' %}
   {% else %}
   	{% set warehouse = var('dictionary_database')+'_INGESTION_WH' %}
   {% endif %}
   {% for tbl in tables %}
      {% do temp.append(task_template | string | replace('{@warehouse}', warehouse) | replace('{@stage_table_name}', tbl.STAGE_TABLE_NAME) | replace('{@source_table_name}', tbl.SOURCE_TABLE_NAME) ) %}
   {% endfor %}

   {% set results = temp | join ('\n') %}
   {{ log(results, info=True) }}
   {% do return(results) %}
{% endmacro %}
--

{% macro generate_csv_file_format() %}
    {% set file_format_name = var('dictionary_file_format_name') %}
    {% set temp=[] %}
    {% do temp.append('') %}
    {% do temp.append('CREATE FILE FORMAT IF NOT EXISTS ' ~ file_format_name) %}
    {% do temp.append('  TYPE = \'CSV\'') %}
    {% do temp.append('  field_delimiter = \'{@field}\'' | replace('{@field}', var('dictionary_field_delimiter') )) %}
    {% do temp.append('  skip_header = ' ~ var('dictionary_skip_header')) %}
    {% do temp.append('  TRIM_SPACE = ' ~ var('dictionary_trim_spaces'))  %}
    {% do temp.append('  ESCAPE_UNENCLOSED_FIELD = NONE ')  %}
    {% do temp.append('  NULL_IF=\'NA\'') %}
    {% do temp.append(';') %}

    {% set results = temp | join ('\n') %}
    {% do return(results) %}
{% endmacro %}
--
{% macro generate_json_file_format() %}
    {% set file_format_name = var('dictionary_file_format_name') %}
    {% set temp=[] %}
    {% do temp.append('') %}
    {% do temp.append('CREATE FILE FORMAT IF NOT EXISTS ' ~ file_format_name) %}
    {% do temp.append('  TYPE = \'JSON\'') %}
    {% do temp.append('  STRIP_OUTER_ARRAY = true') %}
    {% do temp.append(';') %}

    {% set results = temp | join ('\n') %}
    {% do return(results) %}
{% endmacro %}
---
{% macro generate_from_dictionary_external_refresh() %}

    {% set sources_yaml=[] %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('use database ' ~ var('dictionary_database') ~';') %}
 
    {% set query %}
    select DISTINCT source_table_name, stage_table_name from internal.dictionary where database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' order by stage_table_name
    {% endset %}
    {% set tables = run_query(query) %}

    {% for tbl in tables %}
	    {% do sources_yaml.append('Alter external table external.{@table_name} REFRESH;' | replace('{@table_name}', tbl.SOURCE_TABLE_NAME) ) %}
    {% endfor %}

    {% if execute %}

        {% set joined = sources_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}

    {% endif %}

{% endmacro %}
---
{% macro generate_from_dictionary_external_tables(table_only='false') %}

    {% set external_file_format = var('dictionary_external_format') %}
    {% set file_format_name = var('dictionary_file_format_name') %}
    {% set external_stage = var('dictionary_external_stage') %}

    {% set sources_yaml=[] %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('use database ' ~ var('dictionary_database') ~';') %}
    {% do sources_yaml.append('create or replace schema external;') %}
    {% do sources_yaml.append('use schema external;') %}

    {% if table_only=='false' %}
        {% if external_file_format == 'csv' %}
            {% do sources_yaml.append(generate_csv_file_format()) %}
        {% else %}
            {% do sources_yaml.append(generate_json_file_format()) %}
        {% endif %}
    {% endif %}

    {% set query %}
    select DISTINCT source_table_name, stage_table_name from internal.dictionary 
    	where database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
	order by stage_table_name
    {% endset %}
    {% set tables = run_query(query) %}

    {% for tbl in tables %}
        {% if table_only == 'false' %}
            {% do sources_yaml.append('CREATE OR REPLACE EXTERNAL TABLE external.' ~ tbl.SOURCE_TABLE_NAME) %}
        {% else %}
            {% do sources_yaml.append('CREATE OR REPLACE TABLE external.' ~ tbl.SOURCE_TABLE_NAME) %}
        {% endif %}
        {% do sources_yaml.append('(') %}
        {% if table_only == 'false' %}
            {% do sources_yaml.append('    cycle_date date as to_date(SPLIT_PART(metadata$filename, \'/\', 3), \'YYYY-MM-DD\')') %}
        {% else %}
            {% do sources_yaml.append('    cycle_date date') %}
        {% endif %}

        {% set query %}
            select  
	    	source_column_name, source_column_type, lower(stage_column_type) stage_column_type, column_order
		, IFNULL(external_column_name, source_column_name) external_column_name 
		, IFNULL(date_format, '{{ var('dictionary_date_format') }}' ) date_format
            from internal.dictionary 
            where 
                database_name='{{ var('dictionary_database') }}' 
                and version_name='{{ var('dictionary_database_version') }}' 
                and lower(source_table_name)=lower('{{tbl.SOURCE_TABLE_NAME}}' )
            order by column_order
        {% endset %}
        {% set columns=run_query(query) %}
        {% for column in columns %}
            {% if table_only == 'false' %}
                {% if external_file_format == 'csv' %}
                    {% set column_label = 'c' ~ column.COLUMN_ORDER %}
                {% else %}
                    {% set column_label = column.EXTERNAL_COLUMN_NAME %}
                {% endif %}
                {% if column.STAGE_COLUMN_TYPE == 'date' %}
                    {% do sources_yaml.append('	, {@source_column_name} {@data_type} as to_date(value:"{@column_label}"::varchar(100), \'{@date_format}\')' | replace('{@source_column_name}', column.SOURCE_COLUMN_NAME) | replace('{@column_label}', column_label) | replace('{@data_type}',column.STAGE_COLUMN_TYPE)  | replace('{@date_format}',  column.DATE_FORMAT ) )%}
                {% elif column.STAGE_COLUMN_TYPE == 'datetime' %}
                    {% do sources_yaml.append('	, {@source_column_name} {@data_type} as to_timestamp(value:"{@column_label}"::varchar(100), \'{@date_format}\')' | replace('{@source_column_name}', column.SOURCE_COLUMN_NAME) | replace('{@column_label}', column_label) | replace('{@data_type}',column.STAGE_COLUMN_TYPE)  | replace('{@date_format}',  column.DATE_FORMAT ) )%}
                {% elif column.STAGE_COLUMN_TYPE.startswith('binary') %}
                    {% do sources_yaml.append('	, {@source_column_name} {@data_type} as base64_decode_binary(value:"{@column_label}"::varchar(100))' | replace('{@source_column_name}', column.SOURCE_COLUMN_NAME) | replace('{@column_label}', column_label) | replace('{@data_type}',column.STAGE_COLUMN_TYPE)  | replace('{@date_format}',  column.DATE_FORMAT ) )%}
                 {% else %}
                    {% do sources_yaml.append('	, {@source_column_name} {@data_type} as (value:"{@column_label}"::{@data_type})' | replace('{@source_column_name}', column.SOURCE_COLUMN_NAME) | replace('{@column_label}',  column_label) | replace('{@data_type}',column.STAGE_COLUMN_TYPE) ) %}
                {% endif %}
            {% else %}
                {% do sources_yaml.append('	, {@source_column_name} {@data_type}' | replace('{@source_column_name}', column.SOURCE_COLUMN_NAME) | replace('{@data_type}',column.STAGE_COLUMN_TYPE) ) %}
            {% endif %}
        {% endfor %}

    {% set file_pattern = var('dictionary_file_pattern') %}
        {% do sources_yaml.append(')') %}
        {% if table_only == 'false' %}
            {% do sources_yaml.append(' PARTITION BY (cycle_date)' ) %}
            {% do sources_yaml.append(' LOCATION = @' ~ external_stage ) %}
            {% do sources_yaml.append(' FILE_FORMAT = ' ~ file_format_name) %}
            {% do sources_yaml.append(' PATTERN = \'.*/{@upper_table_name}{@file_pattern}\'' | replace('{@upper_table_name}', tbl.SOURCE_TABLE_NAME) | replace('{@file_pattern}', file_pattern)) %}
        {% endif %}
        {% do sources_yaml.append(';' ) %}
        {% do sources_yaml.append('') %}
    {% endfor %}

    {% if execute %}

        {% set joined = sources_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}

    {% endif %}

{% endmacro %}
---
{% macro generate_from_dictionary_source_yml(database_name='default', version_name='default', schema_name='external', generate_columns=True, include_cycle_date=True, include_descriptions=True, include_external=False, source_identifier=True, filter='') %}

    {% set sources_yaml=[] %}

    {% if database_name=='default' %}
        {% set database_name = var('dictionary_database', target.database) %}
    {% endif %}
    {% if version_name=='default' %}
        {% set version_name = var('dictionary_database_version', 'default') %}
    {% endif %}
    {% if schema_name=='default' %}
        {% set version_name = var('dictionary_schema', 'external') %}
    {% endif %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('version: 2') %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('sources:') %}
    {% do sources_yaml.append('  - name: ' ~ database_name | lower ~ '__' ~ schema_name ) %}
    {% do sources_yaml.append('    description: "' ~ var('dictionary_source_description') ~ '"') %}
    {% do sources_yaml.append('    database: "' ~  database_name ~ '"') %}
    {% do sources_yaml.append('    schema:  "' ~ schema_name  ~ '"') %}
    {% do sources_yaml.append('    loader:  Manual') %}

    {% do sources_yaml.append('    tables:') %}
    
    {% set query %}
    	select DISTINCT source_table_name, stage_table_name 
	from internal.dictionary 
	where 
		database_name='{@database_name}' and version_name='{@version_name}' 
		{{ filter }}
	order by stage_table_name
    {% endset %}
    {% set tables = run_query(query | string | replace('{@database_name}', database_name) | replace('{@version_name}', version_name) ) %}

    {% for tbl in tables %}
    	{% if source_identifier %}
		{% do sources_yaml.append('      - name: ""' ~  tbl.SOURCE_TABLE_NAME | lower ~ '"') %}
	{% else %}
 		{% do sources_yaml.append('      - name: ' ~ tbl.STAGE_TABLE_NAME | lower) %}
	{% endif %}
        {% if include_external %}
            {% do sources_yaml.append('        external: ' ) %}
            {% do sources_yaml.append('          location:  "@raw.snowplow.snowplow"' ) %}
            {% do sources_yaml.append('          file_format: "( type = csv )" ' ) %}
            {% do sources_yaml.append('          auto_refresh: true ' ) %}     
        {% endif %}

        {% if generate_columns %}
            {% do sources_yaml.append('        columns:') %}
	    	{% if include_cycle_date %}
			{% do sources_yaml.append('          - name: ' ~ 'cycle_date' ) %}
			{% do sources_yaml.append('            data_type: ' ~ 'date' ) %}
			{% if include_descriptions %}
			    {% do sources_yaml.append('            description: "' ~ 'Date of the production cycle that contained this value' + '"' ) %}
			{% endif %}
               {% endif %}
		
                
                {% set query %}
                    select  
                        source_column_name, stage_column_description, stage_column_name, lower(stage_column_type) stage_column_type
                    from internal.dictionary 
                    where 
                        database_name='{{ var('dictionary_database') }}' 
                        and version_name='{{ var('dictionary_database_version') }}' 
                        and source_table_name='{{tbl.SOURCE_TABLE_NAME}}' 
			{{ filter }}
			order by column_order
                {% endset %}

            {% set columns=run_query(query) %}
            {% for column in columns %}
		{% if source_identifier %}
			{% set column_name = column.SOURCE_COLUMN_NAME %}
		{% else %}
			{% set column_name = column.STAGE_COLUMN_NAME %}
		{% endif %}
		{% do sources_yaml.append('          - name: "' ~ column_name ~ '"') %}
                {% do sources_yaml.append('            data_type: ' ~ column.STAGE_COLUMN_TYPE ) %}
                {% if include_descriptions %}
                    {% do sources_yaml.append('            description: "' ~ column.STAGE_COLUMN_DESCRIPTION + '"' ) %}
                {% endif %}
            {% endfor %}
            {% do sources_yaml.append('') %}
        {% endif %}
    {% endfor %}

    {% if execute %}
        {% set joined = sources_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}

    {% endif %}

{% endmacro %}
--
{% macro generate_from_dictionary_model_yml(schema_name, generate_columns=True, include_descriptions=True) %}

    {% set sources_yaml=[] %}

    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('version: 2') %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('models:') %}

    {% set query %}
    select DISTINCT source_table_name, stage_table_name 
    from internal.dictionary 
    where database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' 
    order by stage_table_name
    {% endset %}
    {% set tables = run_query(query) %}

    {% for tbl in tables %}
        {% do sources_yaml.append('      - name: ' ~ schema_name ~ '__' ~ tbl.STAGE_TABLE_NAME ) %}
        {% do sources_yaml.append('        description: Model description' ) %}
        {% if generate_columns %}
            {% do sources_yaml.append('        columns:') %}
                {% do sources_yaml.append('          - name: ' ~ 'cycle_date' ) %}
                {% do sources_yaml.append('            data_type: ' ~ 'date' ) %}
                {% if include_descriptions %}
                    {% do sources_yaml.append('            description: "' ~ 'Date of the production cycle that contained this value' + '"' ) %}
                {% endif %}

                {% set query %}
                    select  
                        source_column_name, source_column_type, stage_column_description, stage_column_name  
                    from internal.dictionary 
                    where 
                        database_name='{{ var('dictionary_database') }}' 
                        and version_name='{{ var('dictionary_database_version') }}' 
                        and stage_table_name='{{tbl.STAGE_TABLE_NAME}}' 
                        AND stage_column_name is not null 
                    order by column_order
                {% endset %}
		{% if schema_name=='public' %}
			{% set query %}
			    select  
				source_column_name, source_column_type, stage_column_description, stage_column_name  
			    from internal.dictionary 
			    where 
				database_name='{{ var('dictionary_database') }}' 
				and version_name='{{ var('dictionary_database_version') }}' 
				and stage_table_name='{{tbl.STAGE_TABLE_NAME}}' 
				AND stage_column_name is not null and is_public=1
			    order by column_order
			{% endset %}
        	{% endif %}
			
            {% set columns=run_query(query) %}
            {% for column in columns %}
                {% do sources_yaml.append('          - name: ' ~ column.STAGE_COLUMN_NAME ) %}
                {% if include_descriptions %}
                    {% do sources_yaml.append('            description: "' ~ column.STAGE_COLUMN_DESCRIPTION + '"' ) %}
                {% endif %}
            {% endfor %}
            {% do sources_yaml.append('') %}
        {% endif %}
    {% endfor %}

    {% if execute %}
        {% set joined = sources_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}

    {% endif %}

{% endmacro %}
