-- fct_invoices.sql
SELECT
    ft.Accident_Index AS accident_id,
    ft.Accident_Severity AS severity,
    dt.week_day,
    ve.number_vehicle,
    ft.Number_of_Casualties AS number_Casualties,
    ft.Pedestrian_Crossing_Human_Control AS pc_human_control,
    ft.Pedestrian_Crossing_Physical_Facilities AS pc_physical,
    ft.Speed_limit AS speed,
    dt.time_accident,
    dt.year,
    dt.accident_date,
    wc.weather,
    wc.light_condition,
    wc.road_condition,
    ve.vehicle_make,
    ve.vehicle_aga,
    d.driver_age_group,
    d.driver_home_type,
    d.sex,
    ve.skidding_overturning,
    ve.first_Point_Impact
FROM 
    (SELECT * FROM {{ source('retail', 'traffic_data') }}) AS ft
JOIN 
    (SELECT * FROM {{ ref('dim_driver') }}) AS d ON ft.Accident_Index = d.driverID
JOIN 
    (SELECT * FROM {{ ref('dim_datetime') }}) AS dt ON d.driverID = dt.dateID
JOIN 
    (SELECT * FROM {{ ref('dim_Weather') }}) AS wc ON dt.dateID = wc.weatherID
JOIN 
    (SELECT * FROM {{ ref('dim_vehicle') }}) AS ve ON wc.weatherID = ve.vehicleID

