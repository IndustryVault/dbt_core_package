
{% macro configure_target_environment(include_raw=True) %}

use role sysadmin;
{% if include_raw %}
    create database if not exists raw;
    create schema if not exists raw.{{var('dictionary_database')}}; 
    create schema if not exists raw.internal; 

    use role accountadmin;
    grant usage on database raw to role develop_role;
    grant usage on schema raw.{{var('dictionary_database')}} to role develop_role;
    grant select on all tables in schema raw.{{var('dictionary_database')}} to role develop_role;
    grant select on future tables in schema raw.{{var('dictionary_database')}} to role develop_role;
    grant usage on schema raw.internal to role develop_role;
    grant select on all tables in schema raw.internal to role develop_role;
   
    create schema if not exists raw.sigma_upload;
    grant usage on database raw to role report_role;

    grant usage on schema raw.sigma_upload to role report_role;
    grant all on schema raw.sigma_upload to role report_role;
    
    use role securityadmin;
    grant create schema on database raw to role load_role;
    grant usage on database raw to role load_role;
    grant all on database raw  to role load_role;
    grant all privileges on all schemas in database raw to role load_role;
    grant all privileges on future schemas in database raw to role load_role;
    grant all privileges on all tables in database raw to role load_role;
    grant all privileges on future tables in database raw to role load_role;

    grant usage on database raw to role transform_role;
    grant usage on schema raw.{{var('dictionary_database')}} to role transform_role;
    grant select on all tables in schema raw.{{var('dictionary_database')}} to role transform_role;
    grant select on future tables in schema raw.{{var('dictionary_database')}} to role transform_role;
    grant usage on schema raw.internal to role transform_role;
    grant select on all tables in schema raw.internal to role transform_role;
{% endif %}

use role sysadmin;
-- developer profile should point to a uniquely named version of the production database like dbt_[developer name]_[database_name]
create database if not exists {{target.database}};  -- create developer database
create database if not exists {{var('dictionary_database')}}; -- create production database
-- if other databases are needed: QA, UAT they should be created here

use role securityadmin;
grant create schema on database {{target.database}} to role develop_role;
grant usage on database {{target.database}} to role develop_role;
grant all on database {{target.database}}  to role develop_role;
grant all privileges on all schemas in database {{target.database}} to role develop_role;
grant all privileges on future schemas in database {{target.database}} to role develop_role;
grant all privileges on all tables in database {{target.database}} to role develop_role;
grant all privileges on future tables in database {{target.database}} to role develop_role;
 
--  permissions needed to deploy to different environments
grant create schema on database {{var('dictionary_database')}} to role transform_role;
grant usage on database {{var('dictionary_database')}} to role transform_role;
grant all on database {{var('dictionary_database')}}  to role transform_role;
grant all privileges on all schemas in database {{var('dictionary_database')}} to role transform_role;
grant all privileges on future schemas in database {{var('dictionary_database')}} to role transform_role;
grant all privileges on all tables in database {{var('dictionary_database')}} to role transform_role;
grant all privileges on future tables in database {{var('dictionary_database')}} to role transform_role;

-- permissions to the report_role
grant usage on database {{var('dictionary_database')}} to role report_role;
grant usage on schema {{var('dictionary_database')}}.public to role report_role;
grant select on all tables in schema {{var('dictionary_database')}}.public to role report_role;
grant select on future tables in schema {{var('dictionary_database')}}.public to role report_role;

{% endmacro %}
