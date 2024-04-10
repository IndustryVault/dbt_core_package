
{% macro configure_target_environment(include_raw=True) %}

use role sysadmin;
{% if include_raw %}
    create database if not exists raw;
    create schema if not exists raw.{{env_var('DBT_SNOWFLAKE_DATABASE')}}; 
    create schema if not exists raw.internal; 

    use role accountadmin;
    grant usage on database raw to role develop_role;
    grant usage on schema raw.{{env_var('DBT_SNOWFLAKE_DATABASE')}} to role develop_role;
    grant select on all tables in schema raw.{{env_var('DBT_SNOWFLAKE_DATABASE')}} to role develop_role;
    grant select on future tables in schema raw.{{env_var('DBT_SNOWFLAKE_DATABASE')}} to role develop_role;
    grant usage on schema raw.internal to role develop_role;
    grant select on all tables in schema raw.internal to role develop_role;
    
    use role securityadmin;
    grant create schema on database raw to role load_role;
    grant usage on database raw to role load_role;
    grant all on database raw  to role load_role;
    grant all privileges on all schemas in database raw to role load_role;
    grant all privileges on future schemas in database raw to role load_role;
    grant all privileges on all tables in database raw to role load_role;
    grant all privileges on future tables in database raw to role load_role;

    grant usage on database raw to role transform_role;
    grant usage on schema raw.{{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;
    grant select on all tables in schema raw.{{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;
    grant select on future tables in schema raw.{{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;
    grant usage on schema raw.internal to role transform_role;
    grant select on all tables in schema raw.internal to role transform_role;
{% endif %}

use role sysadmin;
-- developer profile should point to a uniquely named version of the production database like dbt_[developer name]_[env_var('DBT_SNOWFLAKE_DATABASE')]
create database if not exists "{{ env_var('DBT_SNOWFLAKE_DEV_PREFIX') ~ env_var('DBT_SNOWFLAKE_DATABASE')}}";  -- create developer database
create database if not exists {{env_var('DBT_SNOWFLAKE_DATABASE')}}; -- create production database
-- if other databases are needed: QA, UAT they should be created here

use role securityadmin;
grant create schema on database "{{ env_var('DBT_SNOWFLAKE_DEV_PREFIX') ~ env_var('DBT_SNOWFLAKE_DATABASE')}}" to role develop_role;
grant usage on database "{{ env_var('DBT_SNOWFLAKE_DEV_PREFIX') ~ env_var('DBT_SNOWFLAKE_DATABASE')}}" to role develop_role;
grant all on database "{{ env_var('DBT_SNOWFLAKE_DEV_PREFIX') ~ env_var('DBT_SNOWFLAKE_DATABASE')}}"  to role develop_role;
grant all privileges on all schemas in database "{{ env_var('DBT_SNOWFLAKE_DEV_PREFIX') ~ env_var('DBT_SNOWFLAKE_DATABASE')}}" to role develop_role;
grant all privileges on future schemas in database "{{ env_var('DBT_SNOWFLAKE_DEV_PREFIX') ~ env_var('DBT_SNOWFLAKE_DATABASE')}}" to role develop_role;
grant all privileges on all tables in database "{{ env_var('DBT_SNOWFLAKE_DEV_PREFIX') ~ env_var('DBT_SNOWFLAKE_DATABASE')}}" to role develop_role;
grant all privileges on future tables in database "{{ env_var('DBT_SNOWFLAKE_DEV_PREFIX') ~ env_var('DBT_SNOWFLAKE_DATABASE')}}" to role develop_role;
 
--  permissions needed to deploy to different environments
grant create schema on database {{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;
grant usage on database {{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;
grant all on database {{env_var('DBT_SNOWFLAKE_DATABASE')}}  to role transform_role;
grant all privileges on all schemas in database {{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;
grant all privileges on future schemas in database {{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;
grant all privileges on all tables in database {{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;
grant all privileges on future tables in database {{env_var('DBT_SNOWFLAKE_DATABASE')}} to role transform_role;

-- permissions to the report_role
grant usage on database {{env_var('DBT_SNOWFLAKE_DATABASE')}} to role report_role;
grant usage on schema {{env_var('DBT_SNOWFLAKE_DATABASE')}}.public to role report_role;
grant select on all tables in schema {{env_var('DBT_SNOWFLAKE_DATABASE')}}.public to role report_role;
grant select on future tables in schema {{env_var('DBT_SNOWFLAKE_DATABASE')}}.public to role report_role;

{% endmacro %}
