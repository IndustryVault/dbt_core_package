  {{ config(materialized='view', sort='the_date', schema='internal') }}
  
  
  WITH generate_dates AS (
    SELECT DATEADD(DAY, SEQ4(), '12/31/1980' ) AS the_date
      FROM TABLE(GENERATOR(ROWCOUNT=>10000 ))  -- Number of days after reference date in previous line
  )
  SELECT the_date
        ,YEAR(the_date)::SMALLINT as year
        ,MONTH(the_date)::SMALLINT AS month
        ,MONTHNAME(the_date)::VARCHAR(3) AS month_abbreviation
        ,TO_CHAR(the_date,'MMMM') as month_name
        ,DAYOFWEEK(the_date) as day_in_week  
        ,CASE WHEN day_in_week=0 OR day_in_week=6 Then True else False End is_weekend
        ,(NOT is_weekend) is_business_day
        ,dayname(the_date)::varchar(50) as day_in_week_abbreviation
        ,['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][day_in_week]::varchar day_of_week_full
        ,WEEK(the_date)::SMALLINT as week_in_year
        ,DAYOFYEAR(the_date)::SMALLINT as day_in_year
        ,DAYOFMONTH(the_date)::SMALLINT as day_in_month
        ,LAST_DAY(the_date, 'month')::date as last_day_in_month
        ,DATE_TRUNC('QUARTER',the_date)::date as first_day_in_quarter
        ,LAST_DAY(the_date, 'quarter')::date as last_day_in_quarter
        ,datediff('day',first_day_in_quarter, the_date) day_in_quarter
        ,datediff('day',first_day_in_quarter, last_day_in_quarter) days_in_quarter
        ,date_from_parts(year,1,1) first_day_in_year
        ,date_from_parts(year,12,31) last_day_in_year
        ,datediff('day',the_date, last_day_in_year) days_remaining_in_year
        ,datediff('day',first_day_in_year, last_day_in_year) days_in_year
        ,LAST_DAY(the_date,'week')::date as last_day_in_week
        ,LAST_DAY(the_date,'year')::date as last_day_in_year
        ,quarter(the_date) as quarter
        ,['First','Second','Third','Fourth'][quarter-1]::varchar quarter_abbreviation
        ,concat(quarter_abbreviation,' Quarter') quarter_name_full
        ,to_varchar(the_date, 'yyyymmdd') date_yyyymmdd        
        ,to_varchar(the_date, 'yyyy-mm-dd') date_yyyy_mm_dd
        ,to_varchar(the_date, 'dd-mon-yyyy') date_dd_mon_yyyy
        ,to_varchar(the_date, 'mm/dd/yyyy') date_mm_dd_yyyy
    FROM generate_dates
