  {{ config(materialized='table', sort='the_date', schema='public') }}
  Select * From {{ ref('vw_date_dimension') }}
