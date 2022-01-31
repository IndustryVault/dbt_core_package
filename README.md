# dbt_core_package
this package contains the following:

## Date_dimension 
a view (vw_date_dimension) that is dynamically populated and ranges from the variable date_dimenion_start_date and continues for the number of days defined in the variable date_dimension_day_count. The dynamic view is then materialized into a table called date_dimension. It is expected that users of this package will reference the table date_dimenion for maximum performance.

## generate_from_dictionary
a set of macros that will generate scaffording for a DBT based a comma separate dictionary seed. The seed must have the following columns defined:
* database_name - supports a dictionary containing multiple databases. Use this field to specifically pull out the database you are generating
* version_name - supports a dictionary containing multiple versions of the same database. Use this field to specify which version
* source_table_name - table name used in the source
* stage_table_name - table name to be used by all staging tables
* source_column_name - column name used in the source
* stage_column_name - column_name to be used by all staging tables
* is_public - should this column be available for use outside of this datbase
* stage_column_description - the description of the column
* stage_column_type - the type information for a column

To use this set of macros you first must create a dictionary that meets the above requirements. You may, of course, have additional information in your dictionary but the above fields are directly referenced by the macros so they must be present. At that point you can run the following commands:
* generate_external_from_dictionary - this generates in the log output the script needed to create the external tables defined in the dictionary
* 
