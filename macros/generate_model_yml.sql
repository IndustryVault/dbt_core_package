{% macro generate_from_dictionary_model_yml(database_name='default', version_name='default', apply_filter='',add_lightdash=false) %}

    {% set sources_yaml=[] %}

    {% if database_name=='default' %}
        {% set database_name = var('dictionary_database', target.database) %}
    {% endif %}
    {% if version_name=='default' %}
        {% set version_name = var('dictionary_database_version', 'default') %}
    {% endif %}

    {% set header %}
#
#   This file was originally generated by the macro generate_from_dictionary_model_yml and is now under manual control. Replacing with a regenerated file will lose changes!!
#

version: 2

models:
   {%- endset -%}
   {% do sources_yaml.append(header | string | replace('(*', '{%') | replace('*)', '%}') | replace('[[', '{{') | replace(']]', '}}') ) %}
    
    {% set query %}
    	select DISTINCT dd.stage_table_name, dd.stage_column_name, dd.column_order, dd.stage_column_description, stage_column_type, source_column_name,
            case when stage_column_type = 'string' then 1
                when stage-column_name = 'as_of_date' then 1
                else 0 end is_dimension
        from internal.data_dictionary dd
        where 
            dd.database_name='{{database_name}}' and dd.version_name='{{version_name}}' 
            {{ apply_filter }}
	    order by dd.stage_table_name, dd.column_order
    {% endset %}
    {% set rowset=run_query(query) %}

    {% set ns = namespace(last_table_name = 'NOT SET') %}
    {% for col in rowset %}
        {% if ns.last_table_name != col.STAGE_TABLE_NAME | string %}
            {% do sources_yaml.append('  - name: ' ~ col.STAGE_TABLE_NAME | lower ) %}
            {% do sources_yaml.append('    columns:') %}
            {% set ns.last_table_name = col.STAGE_TABLE_NAME | string %}
        {%endif %}

        {% do sources_yaml.append('      - name: ' ~ col.STAGE_COLUMN_NAME | lower) %}
        {% do sources_yaml.append('        description: \'{{ doc("' ~ database_name ~ '_' ~ col.STAGE_TABLE_NAME ~ '_' ~ col.STAGE_COLUMN_NAME ~ '_stage_description' ~ '") }}\'' ) %}
        {% if add_lightdash %}
            {% do sources_yaml.append('        meta: ' ) %}
            {% if col.IS_DIMENSION %}
                {% do sources_yaml.append('          dimension: ' ) %}
                {% do sources_yaml.append('            type: ' ~ col.STAGE_COLUMN_TYPE | lower) %}
                {% do sources_yaml.append('            description: \'{{ doc("' ~ database_name ~ '_' ~ col.STAGE_TABLE_NAME ~ '_' ~ col.STAGE_COLUMN_NAME ~ '_stage_description' ~ '") }}\'' ) %}
                {% do sources_yaml.append('            label: "' ~ col.SOURCE_COLUMN_NAME ~ '"') %}
                {% do sources_yaml.append('            hidden: false' ) %}
                {% do sources_yaml.append('            group_label: ' ~ col.STAGE_TABLE_NAME ) %}
            {% else %}
                {% do sources_yaml.append('          metrics: ' ) %}
                {% do sources_yaml.append('            ' ~ col.STAGE_COLUMN_NAME ~ '_count:') %}
                {% do sources_yaml.append('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Count"') %}
                {% do sources_yaml.append('              type: count') %}
                {% do sources_yaml.append('              hidden: false' ) %}
                {% do sources_yaml.append('            ' ~ col.STAGE_COLUMN_NAME ~ '_min:') %}
                {% do sources_yaml.append('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Minimum"') %}
                {% do sources_yaml.append('              type: min') %}
                {% do sources_yaml.append('              hidden: false' ) %}
                {% do sources_yaml.append('            ' ~ col.STAGE_COLUMN_NAME ~ '_max:') %}
                {% do sources_yaml.append('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Maximum"') %}
                {% do sources_yaml.append('              type: max') %}
                {% do sources_yaml.append('              hidden: false' ) %}
                {% if col.STAGE_COLUMN_TYPE == 'number' %}
                    {% do sources_yaml.append('            ' ~ col.STAGE_COLUMN_NAME ~ '_sum:') %}
                    {% do sources_yaml.append('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Sum"') %}
                    {% do sources_yaml.append('              type: sum') %}
                    {% do sources_yaml.append('              hidden: false' ) %}
                    {% do sources_yaml.append('            ' ~ col.STAGE_COLUMN_NAME ~ 'avg:') %}
                    {% do sources_yaml.append('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Average"') %}
                    {% do sources_yaml.append('              type: average') %}
                    {% do sources_yaml.append('              hidden: false' ) %}
                {% endif %}
	        {% endif %}
        {% endif %}
    {% endfor %} 

    {% set joined = sources_yaml | join ('\n') %}
    {{ log(joined, info=True) }}
    {% do return(joined) %}

{% endmacro %}
