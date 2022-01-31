  {{ config(materialized='view', sort='the_date', schema='internal') }}
  
  WITH generate_dates AS (
    SELECT DATEADD(DAY, SEQ4(), {{ var('date_dimension_start_date')}}) AS the_date
      FROM TABLE(GENERATOR(ROWCOUNT=>{{ var('date_dimension_day_count') }} ))  -- Number of days after reference date in previous line
  )
  SELECT the_date
        ,YEAR(the_date)::SMALLINT as year
        ,MONTH(the_date)::SMALLINT AS month
        ,MONTHNAME(the_date)::VARCHAR(3) AS month_name
        ,DAY(the_date)::SMALLINT as day
        ,DAYOFWEEK(the_date)::VARCHAR(9) as day_of_week
        ,WEEKOFYEAR(the_date)::SMALLINT as week_of_year
        ,DAYOFYEAR(the_date)::SMALLINT as day_of_year
    FROM generate_dates
