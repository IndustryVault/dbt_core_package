{% macro generate_from_dictionary_model_yml(database_name='default', version_name='default', description_method='none', apply_filter='',add_lightdash=false) %}

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
   {% do print(header | string | replace('(*', '{%') | replace('*)', '%}') | replace('[[', '{{') | replace(']]', '}}') ) %}
    
    {% set query %}
    	select DISTINCT dd.stage_table_name, dd.stage_column_name, dd.column_order, dd.stage_column_description, stage_column_type, source_column_name
            , case 
                when stage_column_name = 'as_of_date' then 1
	    	when stage_column_type = 'string' then 1
                else 0 
		end is_dimension
	    , case 
	    	when stage_column_name = 'as_of_date' then 0
	    	when stage_column_type != 'string' then 1
                else 0 
		end is_metric
            , IFF(stage_column_type in ('int','number(6,4)','number(20,2)'),1,0) as is_number_type
        from {{ref('data_dictionary')}} dd
        where 
            dd.database_name='{{database_name}}' and dd.version_name='{{version_name}}' and dd.is_public
            {{ apply_filter }}
	    order by dd.stage_table_name, dd.column_order
    {% endset %}
    {% set rowset=run_query(query) %}

    {% set ns = namespace(last_table_name = 'NOT SET') %}
    {% for col in rowset %}
        {% if ns.last_table_name != col.STAGE_TABLE_NAME | string %}
            {% do print('  - name: ' ~ col.STAGE_TABLE_NAME | lower ) %}
            {% do print('    columns:') %}
            {% set ns.last_table_name = col.STAGE_TABLE_NAME | string %}
        {%endif %}

        {% do print('      - name: ' ~ col.STAGE_COLUMN_NAME | lower) %}
	{% if description_method == 'reference' %}
        {% do print('        description: \'{{ doc("' ~ database_name ~ '_' ~ col.STAGE_TABLE_NAME ~ '_' ~ col.STAGE_COLUMN_NAME ~ '_stage_description' ~ '") }}\'' ) %}
	{% elif description_method == 'direct' %}
        {% do print('            description: "' ~ col.STAGE_COLUMN_DESCRIPTION ~ '"' )  %}  
	{% endif %}

        {% if add_lightdash %}
            {% do print('        meta: ' ) %}
            {% if col.IS_DIMENSION %}
                {% do print('          dimension: ' ) %}
                {% do print('            type: ' ~ col.STAGE_COLUMN_TYPE | lower) %}
                {% do print('            label: "' ~ col.SOURCE_COLUMN_NAME ~ '"') %}
                {% do print('            hidden: false' ) %}
            {% else %}
                {% do print('          dimension: ' ) %}
                {% do print('            hidden: true' ) %}
            {% endif %}
            {% if col.IS_METRIC %}            
                {% do print('          metrics: ' ) %}
                {% do print('            ' ~ col.STAGE_COLUMN_NAME ~ '_count:') %}
                {% do print('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Count"') %}
                {% do print('              type: count') %}
                {% do print('              hidden: false' ) %}
                {% do print('              group_label: "' ~ col.SOURCE_COLUMN_NAME ~ '"') %}
                {% do print('            ' ~ col.STAGE_COLUMN_NAME ~ '_min:') %}
                {% do print('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Minimum"') %}
                {% do print('              type: min') %}
                {% do print('              hidden: false' ) %}
                {% do print('              group_label: "' ~ col.SOURCE_COLUMN_NAME ~ '"') %}
                {% do print('            ' ~ col.STAGE_COLUMN_NAME ~ '_max:') %}
                {% do print('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Maximum"') %}
                {% do print('              type: max') %}
                {% do print('              hidden: false' ) %}
                {% do print('              group_label: "' ~ col.SOURCE_COLUMN_NAME ~ '"') %}
                {% if col.IS_NUMBER_TYPE %}
                    {% do print('            ' ~ col.STAGE_COLUMN_NAME ~ '_sum:') %}
                    {% do print('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Sum"') %}
                    {% do print('              type: sum') %}
                    {% do print('              hidden: false' ) %}
                    {% do print('              group_label: "' ~ col.SOURCE_COLUMN_NAME ~ '"') %}
                    {% do print('            ' ~ col.STAGE_COLUMN_NAME ~ 'avg:') %}
                    {% do print('              label: "' ~ col.SOURCE_COLUMN_NAME ~ ' - Average"') %}
                    {% do print('              type: average') %}
                    {% do print('              hidden: false' ) %}
                    {% do print('              group_label: "' ~ col.SOURCE_COLUMN_NAME ~ '"') %}
               {% endif %}
	        {% endif %}
        {% endif %}
    {% endfor %} 

{% endmacro %}
