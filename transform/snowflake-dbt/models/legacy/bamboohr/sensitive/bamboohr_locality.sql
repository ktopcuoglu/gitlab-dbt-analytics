WITH source AS (

    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping_source') }}
    WHERE uploaded_at >= '2020-03-24'
    --1st time we started capturing locality

), intermediate AS (

    SELECT 
      employee_number,
      employee_id,
      CASE
        WHEN locality = 'US Middle Atlantic, United States'
          THEN 'US Mid Atlantic'
        WHEN locality IN ('US New England, United States',
                          'US Middle Atlantic, United States',
                          'US South Atlantic, United States', 
                          'US Pacific, United States',
                          'US Mountain, United States',
                          'US Central, United States')
          THEN SPLIT_PART(locality,',',1)
        
        ELSE locality END                                             AS locality,
      DATE_TRUNC(day, uploaded_at)                                    AS updated_at
    FROM source
    WHERE locality IS NOT NULL

)
 
SELECT *
FROM intermediate

