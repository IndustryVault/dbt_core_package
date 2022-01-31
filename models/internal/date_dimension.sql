  {{ config(materialized='table', sort='the_date', schema='internal') }}
  Select * From {{ ref('vw_date_dimension') }}
