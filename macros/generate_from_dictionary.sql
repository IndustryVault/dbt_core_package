{# 
    marcos related to reading a file format from a dictionary and generating the scaffording of the dbt project. all references from the dictionary are by column name so the
    dictionary must match the same naming style.
#}

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
{% macro generate_external_refresh_from_dictionary() %}

    {% set sources_yaml=[] %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('use database ' ~ var('dictionary_database') ~';') %}
    {% set tables=get_tables_from_dictionary() %}

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
{% macro generate_external_from_dictionary(table_only='false') %}

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

    {% set tables=get_tables_from_dictionary() %}
    {% for tbl in tables %}
        {% if table_only == 'false' %}
            {% do sources_yaml.append('CREATE OR REPLACE EXTERNAL TABLE external.' ~ tbl.SOURCE_TABLE_NAME) %}
        {% else %}
            {% do sources_yaml.append('CREATE OR REPLACE TABLE external.' ~ tbl.SOURCE_TABLE_NAME) %}
        {% endif %}
        {% do sources_yaml.append('(') %}
        {% if table_only == 'false' %}
            {% do sources_yaml.append('    cycle_date date as to_date(CONCAT(substring(metadata$filename, charindex(\'/fay/\',metadata$filename)+5, 4), substring(metadata$filename, charindex(\'/fay/\',metadata$filename)+10, 2), substring(metadata$filename, charindex(\'/fay/\',metadata$filename)+13, 2)), \'YYYYMMDD\')') %}
        {% else %}
            {% do sources_yaml.append('    cycle_date date') %}
        {% endif %}

        {% set query %}
            select  source_column_name, source_column_type, lower(stage_column_type) stage_column_type, column_order, IFNULL(external_column_name, source_column_name) external_column_name 
            from internal.dictionary 
            where 
                database_name='{{ var('dictionary_database') }}' 
                and version_name='{{ var('dictionary_database_version') }}' 
                and lower(source_table_name)=lower('{{tbl.SOURCE_TABLE_NAME}}' )
            order by column_order
        {% endset %}
        {% set columns=run_query(query) %}
        {% set dateformat = var('dictionary_date_format') %}
        {% for column in columns %}
            {% if table_only == 'false' %}
                {% if external_file_format == 'csv' %}
                    {% set column_label = 'c' ~ column.COLUMN_ORDER %}
                {% else %}
                    {% set column_label = column.EXTERNAL_COLUMN_NAME %}
                {% endif %}
                {% if column.STAGE_COLUMN_TYPE == 'date' %}
                    {% do sources_yaml.append('	, {@source_column_name} {@data_type} as to_date(value:"{@column_label}"::varchar(100), \'{@date_format}\')' | replace('{@source_column_name}', column.SOURCE_COLUMN_NAME) | replace('{@column_label}', column_label) | replace('{@data_type}',column.STAGE_COLUMN_TYPE)  | replace('{@date_format}',  dateformat ) )%}
                {% elif column.STAGE_COLUMN_TYPE == 'datetime' %}
                    {% do sources_yaml.append('	, {@source_column_name} {@data_type} as to_timestamp(value:"{@column_label}"::varchar(100), \'{@date_format}\')' | replace('{@source_column_name}', column.SOURCE_COLUMN_NAME) | replace('{@column_label}', column_label) | replace('{@data_type}',column.STAGE_COLUMN_TYPE)  | replace('{@date_format}',  dateformat ) )%}
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
            {% do sources_yaml.append('WITH') %}
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
--
{% macro generate_raw_from_dictionary() %}

    {% set sources_yaml=[] %}
    {% do sources_yaml.append('') %}
    {% set tables=get_tables_from_dictionary() %}

    {% for tbl in tables %}
        {% do sources_yaml.append('CREATE OR REPLACE TABLE raw.' ~ tbl.SOURCE_TALBE_NAME) %}
        {% do sources_yaml.append('(') %}
        {% do sources_yaml.append('     _uid bigint identity(1,1)') %}
        {% do sources_yaml.append('     , cycle_datecycle_date date not null') %}

        {% set query %}
            select  
                source_column_name, source_column_type, stage_column_name, lower(stage_column_type) stage_column_type  
            from internal.dictionary 
            where 
                database_name='{{ var('dictionary_database') }}' 
                and version_name='{{ var('dictionary_database_version') }}' 
                and source_table_name='{{tbl.SOURCE_COLUMN_NAME}}' 
            order by column_order
        {% endset %}

        {% set columns=run_query(query) %}
        {% for column in columns %}
            {% do sources_yaml.append('     , {@source_column_name} {@data_type} null' | replace('{@source_column_name}', column.SOURCE_TABLE_NAME) | replace('{@data_type}',column.STAGE_COLUMN_TYPE) ) %}
        {% endfor %}

        {% do sources_yaml.append(');') %}
        {% do sources_yaml.append('') %}
    {% endfor %}

    {% if execute %}

        {% set joined = sources_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}

    {% endif %}

{% endmacro %}
--
{% macro get_latest_version_from_dictionary() %}

    {% set query %}
        select to_char(MAX(version_name)) latest_version_name from internal.dictionary where database_name='{{ var('dictionary_database') }}'
    {% endset %}
    {% set results = run_query(query) %}
        {{ log('get_latest_version_from_dictionary - ' ~ results[0].LATEST_VERSION_NAME, info=True) }}
    {{ return(results[0].LATEST_VERSION_NAME)}}

{% endmacro %}
--
{% macro get_tables_from_dictionary() %}

    {% set query %}
    select DISTINCT source_table_name, stage_table_name from internal.dictionary where database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}' order by stage_table_name
    {% endset %}
    {% set table_list = run_query(query) %}

    {{ return(table_list) }}

{% endmacro %}
--
{% macro get_source_table_from_dictionary(stage_table_name) %}

    {% set query %}
    select DISTINCT source_table_name, stage_table_name from internal.dictionary 
        where stage_table_name='{{stage_table_name}}' and database_name='{{ var('dictionary_database') }}' and version_name='{{ var('dictionary_database_version') }}'
    {% endset %}
    {% set results = run_query(query) %}
    {{ log('get_source_table_from_dictionary - ' ~ results.SOURCE_TABLE_NAME, info=True) }}

    {% if execute %}
    {{ return(results.SOURCE_TABLE_NAME | string) }}
{% else %}
  {{ return('') }}
{% endif %}

{% endmacro %}
---
{% macro generate_source_from_dictionary(generate_columns=True, include_descriptions=True, include_external=False) %}

    {% set sources_yaml=[] %}

    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('version: 2') %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('sources:') %}
    {% do sources_yaml.append('  - name: ' ~ var('dictionary_database') | lower ) %}
    {% do sources_yaml.append('    description: "Black Knight Financial Services (BKFS) supplied export of the MSP system. Received daily except for Sunday and Monday."') %}
    {% do sources_yaml.append('    database: "{{ var(''dictionary_database'', target.database) }}"') %}
    {% do sources_yaml.append('    schema:  "{{ var(''dictionary_schema'', \'external\') }}"') %}
    {% do sources_yaml.append('    loader:  Manual') %}

    {% do sources_yaml.append('    tables:') %}

    {% set tables=get_tables_from_dictionary() %}

    {% for tbl in tables %}
        {% do sources_yaml.append('      - name: ' ~ 'external__' ~ tbl.SOURCE_TABLE_NAME | lower) %}
        {% do sources_yaml.append('        identifier: ' ~ tbl.SOURCE_TABLE_NAME ) %}
        {% if include_external %}
            {% do sources_yaml.append('        external: ' ) %}
            {% do sources_yaml.append('          location:  "@raw.snowplow.snowplow"' ) %}
            {% do sources_yaml.append('          file_format: "( type = csv )" ' ) %}
            {% do sources_yaml.append('          auto_refresh: true ' ) %}     
        {% endif %}

        {% if generate_columns %}
            {% do sources_yaml.append('        columns:') %}
                {% do sources_yaml.append('          - name: ' ~ 'cycle_date' ) %}
                {% do sources_yaml.append('            data_type: ' ~ 'date' ) %}
                {% if include_descriptions %}
                    {% do sources_yaml.append('            description: "' ~ 'Date of the production cycle that contained this value' + '"' ) %}
                {% endif %}
                
                {% set query %}
                    select  
                        source_column_name, source_column_type, stage_column_description, stage_column_name, lower(stage_column_type) stage_column_type
                    from internal.dictionary 
                    where 
                        database_name='{{ var('dictionary_database') }}' 
                        and version_name='{{ var('dictionary_database_version') }}' 
                        and source_table_name='{{tbl.SOURCE_TABLE_NAME}}' 
                    order by column_order
                {% endset %}

            {% set columns=run_query(query) %}
            {% for column in columns %}
                {% do sources_yaml.append('          - name: ' ~ column.SOURCE_COLUMN_NAME ) %}
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
{% macro generate_model_from_dictionary(schema_name, generate_columns=True, include_descriptions=True) %}

    {% set sources_yaml=[] %}

    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('version: 2') %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('models:') %}

    {% set tables=get_tables_from_dictionary() %}

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
