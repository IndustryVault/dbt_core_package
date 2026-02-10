{{ config(materialized='table', sort='the_date', schema='public') }}



WITH generate_dates AS (
  SELECT DATEADD(DAY, SEQ4(), DATEADD(DAY,-15000, current_date()) ) AS the_date
        ,DAYOFWEEK(the_date) as day_in_week  
        ,CASE WHEN day_in_week=0 OR day_in_week=6 Then True else False End is_weekend
        ,(NOT is_weekend) is_business_day
        ,CASE WHEN day_in_week=0 Then False else True End is_operation_day
    FROM TABLE(GENERATOR(ROWCOUNT=>30000 ))  -- Number of days after reference date in previous line
)
, business_dates as 
(
    SELECT 
        The_date 
        ,LEAD(THE_DATE,1) OVER(ORDER BY THE_DATE ASC) NEXT_BUSINESS_DATE
        ,LEAD(THE_DATE,3) OVER(ORDER BY THE_DATE ASC) plus3_business_days
        ,LEAD(THE_DATE,5) OVER(ORDER BY THE_DATE ASC) plus5_business_days
        ,LEAD(THE_DATE,10) OVER(ORDER BY THE_DATE ASC) plus10_business_days
        ,LEAD(THE_DATE,15) OVER(ORDER BY THE_DATE ASC) plus15_business_days
        ,LEAD(THE_DATE,20) OVER(ORDER BY THE_DATE ASC) plus20_business_days
        ,LEAD(THE_DATE,30) OVER(ORDER BY THE_DATE ASC) plus30_business_days
    FROM generate_dates 
    WHERE IS_BUSINESS_DAY and the_date not in (Select holiday_date from cmg_master.public.holidays)
)
, business_days_remaining_in_month as
(
    select generate_dates.the_date, count(*) day_count
    From generate_dates
    left join business_dates on business_dates.the_date > generate_dates.the_date and business_dates.the_date <= LAST_DAY(generate_dates.the_date,'month')
    group by all
)
, business_days_remaining_in_quarter as
(
    select generate_dates.the_date, count(*) day_count
    From generate_dates
    left join business_dates on business_dates.the_date > generate_dates.the_date and business_dates.the_date <= LAST_DAY(generate_dates.the_date,'quarter')
    group by all
)
, business_days_remaining_in_year as
(
    select generate_dates.the_date, count(*) day_count
    From generate_dates
    left join business_dates on business_dates.the_date > generate_dates.the_date and business_dates.the_date <= LAST_DAY(generate_dates.the_date,'year')
    group by all
)

, operation_dates as 
(
    SELECT 
        The_date
        ,LEAD(THE_DATE,1) OVER(ORDER BY THE_DATE ASC) NEXT_OPERATION_DATE
        ,LEAD(THE_DATE,3) OVER(ORDER BY THE_DATE ASC) Plus3_Operation_Days
        ,LEAD(THE_DATE,5) OVER(ORDER BY THE_DATE ASC) Plus5_operation_days
        ,LEAD(THE_DATE,10) OVER(ORDER BY THE_DATE ASC) plus10_operation_days
        ,LEAD(THE_DATE,15) OVER(ORDER BY THE_DATE ASC) plus15_operation_days
        ,LEAD(THE_DATE,20) OVER(ORDER BY THE_DATE ASC) plus20_operation_days
        ,LEAD(THE_DATE,30) OVER(ORDER BY THE_DATE ASC) plus30_operation_days
    FROM generate_dates 
    WHERE IS_operation_DAY and the_date not in (Select holiday_date from cmg_master.public.holidays)
)
, operation_days_remaining_in_month as
(
    select generate_dates.the_date, count(*) day_count
    From generate_dates
    left join operation_dates on operation_dates.the_date > generate_dates.the_date and operation_dates.the_date <= LAST_DAY(generate_dates.the_date,'month')
    group by all
)
, operation_days_remaining_in_quarter as
(
    select generate_dates.the_date, count(*) day_count
    From generate_dates
    left join operation_dates on operation_dates.the_date > generate_dates.the_date and operation_dates.the_date <= LAST_DAY(generate_dates.the_date,'quarter')
    group by all
)
, operation_days_remaining_in_year as
(
    select generate_dates.the_date, count(*) day_count
    From generate_dates
    left join operation_dates on operation_dates.the_date > generate_dates.the_date and operation_dates.the_date <= LAST_DAY(generate_dates.the_date,'year')
    group by all
)
SELECT cal.the_date
      ,YEAR(cal.the_date)::SMALLINT as year
      ,MONTH(cal.the_date)::SMALLINT AS month
      ,MONTHNAME(cal.the_date)::VARCHAR(3) AS month_abbreviation
      ,TO_CHAR(cal.the_date,'MMMM') as month_name
      ,dayname(cal.the_date)::varchar(50) as day_in_week_abbreviation
      ,['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][day_in_week]::varchar day_of_week_full
      ,WEEK(cal.the_date)::SMALLINT as week_in_year
      ,DAYOFYEAR(cal.the_date)::SMALLINT as day_in_year
      ,DAYOFMONTH(cal.the_date)::SMALLINT as day_in_month
      ,LAST_DAY(cal.the_date, 'month')::date as last_day_in_month
      ,DATE_TRUNC('QUARTER',cal.the_date)::date as first_day_in_quarter
      ,LAST_DAY(cal.the_date, 'quarter')::date as last_day_in_quarter
      ,datediff('day',first_day_in_quarter, cal.the_date) day_in_quarter
      ,datediff('day',first_day_in_quarter, last_day_in_quarter) days_in_quarter
      ,date_from_parts(year,1,1) first_day_in_year
      ,date_from_parts(year,12,31) last_day_in_year
      ,datediff('day',cal.the_date, last_day_in_year) days_remaining_in_year
      ,datediff('day',first_day_in_year, last_day_in_year)+1 days_in_year
      ,LAST_DAY(cal.the_date,'week')::date as last_day_in_week
      ,quarter(cal.the_date) as quarter
      ,['First','Second','Third','Fourth'][quarter-1]::varchar quarter_abbreviation
      ,concat(quarter_abbreviation,' Quarter') quarter_name_full
      ,to_varchar(cal.the_date, 'yyyymmdd') format_yyyymmdd        
      ,to_varchar(cal.the_date, 'yyyy-mm-dd') format_yyyy_mm_dd
      ,to_varchar(cal.the_date, 'dd-mon-yyyy') format_dd_mon_yyyy
      ,to_varchar(cal.the_date, 'mm/dd/yyyy') format_mm_dd_yyyy
      ,plus3_business_days as BUSINESSDATE03
      ,plus5_business_days as BUSINESSDATE05
      ,plus10_business_days as BUSINESSDATE10
      ,plus15_business_days as  BUSINESSDATE15
      ,plus20_business_days as BUSINESSDATE20
      ,plus30_business_days as BUSINESSDATE30
      , business_dates.* exclude (the_date)
      , operation_dates.* exclude (the_date)
      ,bmonth.day_count as business_days_remaining_in_month
      ,bquarter.day_count as business_days_remaining_in_quarter
      ,byear.day_count as business_days_remaining_in_year
      ,omonth.day_count as operation_days_remaining_in_month
      ,oquarter.day_count as operation_days_remaining_in_quarter
      ,oyear.day_count as operation_days_remaining_in_year
FROM generate_dates cal
ASOF JOIN business_dates MATCH_CONDITION ( cal.the_date <= business_dates.The_date )
JOIN business_days_remaining_in_month bmonth on cal.the_date=bmonth.the_date  
JOIN business_days_remaining_in_quarter bquarter on cal.the_date=bquarter.the_date
JOIN business_days_remaining_in_year byear on cal.the_date=byear.the_date
ASOF JOIN operation_dates MATCH_CONDITION ( cal.the_date <= operation_dates.The_date )
JOIN operation_days_remaining_in_month omonth on cal.the_date=omonth.the_date  
JOIN operation_days_remaining_in_quarter oquarter on cal.the_date=oquarter.the_date
JOIN operation_days_remaining_in_year oyear on cal.the_date=oyear.the_date
where cal.the_date >= '2026-01-01' 
order by cal.the_date asc;
