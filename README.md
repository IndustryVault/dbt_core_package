
# dbt_core_package
this package contains the following:

## date_dimension 
a view (vw_date_dimension) that is dynamically populated and ranges from the variable date_dimenion_start_date and continues for the number of days defined in the variable date_dimension_day_count. The dynamic view is then materialized into a table called date_dimension. It is expected that users of this package will reference the table date_dimenion for maximum performance.

## generate_from_dictionary
a set of macros that will generate scaffolding for a DBT based a comma separate dictionary seed. The seed must have the following columns defined:
* database_name - supports a dictionary containing multiple databases. Use this field to specifically pull out the database you are generating
* version_name - supports a dictionary containing multiple versions of the same database. Use this field to specify which version
* source_table_name - table name used in the source
* stage_table_name - table name to be used by all staging tables
* source_column_name - column name used in the source
* stage_column_name - column_name to be used by all staging tables
* is_public - should this column be available for use outside of this database
* stage_column_description - the description of the column
* stage_column_type - the type information for a column
* allow_null - whether the source data allows null values in this column
* raw_schema - a variable set in every project which refers to the table name in raw you want to derive the data_dictionary from

To use this set of macros you first must create a dictionary (the model is referenced by ```{{ref('data_dictionary')}}```) that meets the above requirements. You may, of course, have additional information in your dictionary but the above fields are directly referenced by the macros so they must be present.  

The data dictionary is assumed be to mostly provided by the external data source provider and, that specification, can be added as a seed in the project that is including this package. Then you write a model that converts that specification into the data_dictionary defined above.

### Data Mapping
When developing the model to represent the data_dictionary you should consider using a data_mapping seed file (data_mapping.csv). 

The data mapping table allows you to define new column names and data types to be applied automatically when the data_dictionary model is built. The purpose is this feature is to pull out of the data_dictionary any column specific handling, which is expensive to maintain in code, into a simple table. To use this feature, simply enter the one of the following scenarios in the data_mapping seed table (```seeds/data_mapping.csv```)
1. Table renaming - Enter the source table name and the desired stage table name. In the data dictionary model we apply a standard formula to change the source table names from camel case to snake case but do no other changes. If you wish to standardize the table same simply enter a row mapping the source table name to the desired table name in the data_mapping.csv seed file
2. Column Renaming - There are two options. If you wish to rename a specific tables, specific column then add a row that includes the source table name and source column name and then the desired new name in the stage_column_name column of the data_mapping table. This will only apply to that specific table. If you wish to rename a column found in multiple tables then leave the source_table_name column as null and for all tables, if it finds a column name that matches the source_column_name it will rename to the stage_column_name. No attempt is made to enforce uniqueness or name validity by this process but dbt will enforce those requirements.
3. Column type - When configuring the data_dictionary model, you assign snowflake based types based on the source data's type. If the source provider says a columns contains a integer then you might map it to a snowflake integer. The goal of that process is to get the data to load into snowflake. But you may be provided columns of data that are currently in the system under a different data type. Instead of putting these unique assignments into the data_dictionary model, use the data_mapping facility to change the data type of any column. Simply enter the source table and column names and then the desired stage_column_type.
4. Column excluded - If a column of data is not to be passed on to the public schema then place a 'Y' in the is_excluded column and that data element will still be loaded in the raw table but will not be included in the source databases public schema.


## Macros
At that point you can run the following commands:

* `dbt run-operation generate_from_dictionary_external_tables` - build the script in the log output (no ability to write to a file in this macro language) that when executed will create the external tables that are the underpinning of this project. This has an optional argument that will instead generate normal tables. This was needed in working in an environment that did not have access to the raw JSON files
* `dbt run-operation generate_from_dictionary_external_tables --args '{table_only: true}'` - build the script in the log output (no ability to write to a file in this macro language) that when executed will create the external tables that are the underpinning of this project. This has an optional argument that will instead generate normal tables. This was needed in working in an environment that did not have access to the raw JSON files
* `dbt run-operation generate_from_dictionary_external_refresh` - builds the refresh script to update the data in the external tables
* `dbt run-operation generate_from_dictionary_source_yml` - builds the sources yml file (again, in the log output) that is copied and pasted into bde_src.yml
* `dbt run-operation generate_from_dictionary_model_yml` - builds the model yml file in the log output that is copied and paster into bde_portfolio.yml
