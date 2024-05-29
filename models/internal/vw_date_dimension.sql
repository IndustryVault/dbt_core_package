  {{ config(materialized='view', sort='the_date', schema='internal') }}
  
  WITH generate_dates AS (
    SELECT DATEADD(DAY, SEQ4(), '{{ var('date_dimension_start_date', '12/31/9999') }}' ) AS the_date
      FROM TABLE(GENERATOR(ROWCOUNT=>{{ var('date_dimension_day_count',0) }} ))  -- Number of days after reference date in previous line
  )
  SELECT the_date
        ,YEAR(the_date)::SMALLINT as year
        ,MONTH(the_date)::SMALLINT AS month
        ,MONTHNAME(the_date)::VARCHAR(3) AS month_abbreviation
        ,TO_CHAR(the_date,'MMMM') as month_name
        ,DAY(the_date)::SMALLINT as day
        ,DAYOFWEEK(the_date) as day_of_week        
        ,dayname(the_date)::varchar(50) as day_of_week_abbreviation
        ,['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][day_of_week]::varchar day_of_week_full
        ,WEEKOFYEAR(the_date)::SMALLINT as week_of_year
        ,DAYOFYEAR(the_date)::SMALLINT as day_of_year
        ,DAYOFMONTH(the_date)::SMALLINT as day_of_month
        ,LAST_DAY(the_date, 'month')::date as last_day_of_month
        ,DATE_TRUNC('QUARTER',the_date)::date as first_day_of_quarter
        ,LAST_DAY(the_date, 'quarter')::date as last_day_of_quarter
        ,datediff('day',first_day_of_quarter, the_date) day_in_quarter
        ,LAST_DAY(the_date,'week')::date as last_day_of_week
        ,LAST_DAY(the_date,'year')::date as last_day_of_year
        ,quarter(the_date) as quarter
        , concat(['First','Second','Third','Fourth'][quarter-1]::varchar,' Quarter') quarter_name_full
        ,to_varchar(the_date, 'yyyy-mm-dd') formatted_date_110
    FROM generate_dates
