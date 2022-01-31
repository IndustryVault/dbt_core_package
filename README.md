# dbt_core_package
this package contains the following:

## Date_dimension 
a view (vw_date_dimension) that is dynamically populated and ranges from the variable date_dimenion_start_date and continues for the number of days defined in the variable date_dimension_day_count. The dynamic view is then materialized into a table called date_dimension. It is expected that users of this package will reference the table date_dimenion for maximum performance.
