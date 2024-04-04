
  
    

    create or replace table `data-zoom-camp-ugo`.`retail`.`dim_vehicle`
    
    

    OPTIONS()
    as (
      -- Create the dimension table
-- SELECT 
--     a.Number_of_Vehicles AS number_vehicle,
--     v.make
--     v.Age_of_Vehicle as vehicle_aga
-- FROM `data-zoom-camp-ugo`.`retail`.`traffic_data` as a 
-- JOIN `data-zoom-camp-ugo`.`retail`.`vehicle_data` as v 
-- ON a.Accident_Index = v.Accident_Index
SELECT 
    a.Accident_Index AS vehicleID,  -- Selecting and aliasing the Accident_Index column as vehicleID
    a.Number_of_Vehicles AS number_vehicle,  -- Selecting the Number_of_Vehicles column
    v.make AS vehicle_make,  -- Selecting the make column from the vehicle_data table and aliasing it as vehicle_make
    v.Age_of_Vehicle as vehicle_aga,  -- Selecting the Age_of_Vehicle column and aliasing it as vehicle_aga
    v.Skidding_and_Overturning AS skidding_overturning,  -- Selecting the Skidding_and_Overturning column
    v.X1st_Point_of_Impact AS first_Point_Impact  -- Selecting the X1st_Point_of_Impact column
FROM 
    `data-zoom-camp-ugo`.`retail`.`traffic_data` AS a  -- Selecting data from the traffic_data table and aliasing it as 'a'
JOIN 
    `data-zoom-camp-ugo`.`retail`.`vehicle_data` AS v  -- Selecting data from the vehicle_data table and aliasing it as 'v'
ON 
    a.Accident_Index = v.Accident_Index  -- Joining the two tables based on the Accident_Index column
    );
  