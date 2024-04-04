
  
    

    create or replace table `data-zoom-camp-ugo`.`retail`.`dim_datetime`
    
    

    OPTIONS()
    as (
      -- dim_datetime.sql

 
-- SELECT
--     Accident_Index AS dateID
--     Year AS year,
--     MONTH(Date) AS month,
--     Day_of_Week AS week_day,
--     DAY(Date) AS day,
--     Time as time
--   FROM `data-zoom-camp-ugo`.`retail`.`traffic_data`
--   WHERE Accident_Index IS NOT NULL

SELECT
    Accident_Index AS dateID,
    Year AS year,
    Day_of_Week AS week_day,
    Date AS accident_date,
    Time AS time_accident
FROM
    `data-zoom-camp-ugo`.`retail`.`traffic_data`
WHERE
    Accident_Index IS NOT NULL
    );
  