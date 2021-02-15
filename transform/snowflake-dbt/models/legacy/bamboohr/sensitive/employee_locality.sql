{{ simple_cte([
    ('locality','bamboohr_locality'),
    ('location_factor_yaml','location_factors_yaml_historical'),
    ('temporary_sheetload', 'sheetload_location_factor_temporary_2020_december')
]) }}

, location_factor_yaml AS (

    SELECT *,
      LOWER(TRIM(area ||', '||country)) AS yaml_locality     
    FROM location_factor_yaml

), location_factor_yaml_everywhere_else AS (

    SELECT *
    FROM location_factor_yaml
    WHERE LOWER(area) LIKE '%everywhere else%'

), location_factor_mexico AS (

    SELECT *
    FROM location_factor_yaml
    WHERE country ='Mexico'
      AND area IN ('Everywhere else','All')

), geozones AS (

    SELECT DISTINCT
      geozone_title, 
      geozone_factor, 
    uploaded_at                    AS valid_from_date
    FROM {{ ref('geozones_yaml_source') }}    
    WHERE state_or_province IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY geozone_title, geozone_factor ORDER BY uploaded_at) =1
  
), geozones_modified AS (
  
    SELECT *,
        COALESCE(
            LAG(valid_from_date) OVER (PARTITION BY geozone_title, geozone_factor ORDER BY valid_from_date),
            {{max_date_in_bamboo_analyses()}})                   AS valid_to_date
    FROM geozones

), final AS (

    SELECT 
      locality.employee_number,
      locality.employee_id,
      locality.updated_at,
      locality.locality                                                 AS bamboo_locality,
      CASE 
        WHEN locality ='US Director Minimum'
          THEN 0.7
        WHEN locality = 'Global Director Minimum' 
          THEN 0.8
        WHEN DATE_TRUNC(month, updated_at) IN ('2020-12-01','2021-01-01')
          THEN temporary_sheetload.location_factor 
        ---using sheetload file for these 2 months as we are still using FY21 location factors, but the yaml file reflects the change for FY22 in 2021.01
        ELSE      
         COALESCE(location_factor_yaml.location_factor/100, 
                  location_factor_yaml_everywhere_else.location_factor/100,
                  location_factor_mexico.location_factor/100,
                  geozones_modified.geozone_factor) END              AS location_factor
    FROM locality
    LEFT JOIN location_factor_yaml
      ON LOWER(locality.locality) = LOWER(location_factor_yaml.yaml_locality)
      AND locality.updated_at BETWEEN location_factor_yaml.valid_from_date AND location_factor_yaml.valid_to_date
    LEFT JOIN location_factor_yaml_everywhere_else
      ON LOWER(locality.locality) = LOWER(CONCAT(location_factor_yaml_everywhere_else.area,', ', location_factor_yaml_everywhere_else.country))
      AND locality.updated_at BETWEEN location_factor_yaml_everywhere_else.valid_from_date AND location_factor_yaml_everywhere_else.valid_to_date
    LEFT JOIN location_factor_mexico
      ON IFF(locality.locality LIKE '%Mexico%', 'Mexico',NULL) = location_factor_mexico.country
      AND locality.updated_at BETWEEN location_factor_mexico.valid_from_date AND location_factor_mexico.valid_to_date
    --The naming convention for Mexico is all in bamboohr and everywhere else in the location factor yaml file accounting for these cases by making this the last coalesce
     LEFT JOIN geozones_modified
      ON locality.locality = geozones_modified.geozone_title 
      AND locality.updated_at BETWEEN geozones_modified.valid_from_date AND geozones_modified.valid_to_date
    LEFT JOIN temporary_sheetload
        ON locality.employee_number = temporary_sheetload.employee_number
        AND DATE_TRUNC(month, locality.updated_at) IN ('2020-12-01','2021-01-01')

)

SELECT *
FROM final