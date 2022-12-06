
{% macro configure_target_environment() %}

use role sysadmin;
create database if not exists raw;
create schema if not exists raw.{{var('dictionary_database')}}; 
create schema if not exists raw.internal; 

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
 
grant usage on database raw to role develop_role;
grant usage on schema raw.{{var('dictionary_database')}} to role develop_role;
grant select on all tables in schema raw.{{var('dictionary_database')}} to role develop_role;
grant usage on schema raw.internal to role develop_role;
grant select on all tables in schema raw.internal to role develop_role;

--  permissions needed to deploy to different environments
grant create schema on database {{var('dictionary_database')}} to role transform_role;
grant usage on database {{var('dictionary_database')}} to role transform_role;
grant all on database {{var('dictionary_database')}}  to role transform_role;
grant all privileges on all schemas in database {{var('dictionary_database')}} to role transform_role;
grant all privileges on future schemas in database {{var('dictionary_database')}} to role transform_role;
grant all privileges on all tables in database {{var('dictionary_database')}} to role transform_role;
grant all privileges on future tables in database {{var('dictionary_database')}} to role transform_role;

grant usage on database raw to role transform_role;
grant usage on schema raw.{{var('dictionary_database')}} to role transform_role;
grant select on all tables in schema raw.{{var('dictionary_database')}} to role transform_role;
grant usage on schema raw.internal to role transform_role;
grant select on all tables in schema raw.internal to role transform_role;
{% endmacro %}
