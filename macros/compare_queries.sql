
{% macro compare_queries(a_query, b_query, primary_key=None) %}

with a as (
    {{ a_query }}
)
, b as (
    {{ b_query }}
)
, a_except_b as (

    select 'a_except_b' comparison, * from a
    {{ dbt.except() }}
    select 'a_except_b' comparison, * from b

)
, b_except_a as (

    select 'b_except_a' comparison, * from b
    {{ dbt.except() }}
    select 'b_except_a' comparison, * from a
),

all_exceptions as (
    select
        *
    from a_except_b

    union all

    select
        *
    from b_except_a
)

Select 
    * 
From all_exceptions 
order by {{ primary_key ~ ", " if primary_key is not none }} 2, 1

{% endmacro %}
