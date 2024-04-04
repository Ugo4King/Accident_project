-- weather condition

-- SELECT
--     Accident_Index AS weatherID
--     Weather_Conditions AS weather,
--     Light_Conditions AS light_condition,
--     Road_Surface_Conditions AS road_condition

-- FROM `data-zoom-camp-ugo`.`retail`.`traffic_data`
-- WHERE Accident_Index IS NOT NULL
SELECT
    Accident_Index AS weatherID,
    Weather_Conditions AS weather,
    Light_Conditions AS light_condition,
    Road_Surface_Conditions AS road_condition
FROM
    `data-zoom-camp-ugo`.`retail`.`traffic_data`
WHERE
    Accident_Index IS NOT NULL