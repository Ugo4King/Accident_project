
  
    

    create or replace table `data-zoom-camp-ugo`.`retail`.`dim_driver`
    
    

    OPTIONS()
    as (
      -- dim_driver.sql

-- Create the dimension table

SELECT
	Accident_Index AS driverID,
	Age_Band_of_Driver AS driver_age_group,
    Driver_Home_Area_Type AS driver_home_type,
    Sex_of_Driver AS sex
FROM `data-zoom-camp-ugo`.`retail`.`vehicle_data`
WHERE Accident_Index IS NOT NULL
    );
  