WITH source AS (
  
    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping_source') }}
    WHERE uploaded_row_number_desc = 1

), intermediate AS (

    SELECT 
      employee_number,
      employee_id,
      first_name,
      last_name,
      hire_date,
      termination_date,
      CASE 
        WHEN age BETWEEN 18 AND 24 THEN '18-24'
        WHEN age BETWEEN 25 AND 29  THEN '25-29'
        WHEN age BETWEEN 30 AND 34  THEN '30-34'
        WHEN age BETWEEN 35 AND 39  THEN '35-39'
        WHEN age BETWEEN 40 AND 44  THEN '40-44'
        WHEN age BETWEEN 44 AND 49  THEN '44-49'
        WHEN age BETWEEN 50 AND 54  THEN '50-54'
        WHEN age BETWEEN 55 AND 59  THEN '55-59'
        WHEN age>= 60               THEN '60+'
        WHEN age IS NULL            THEN 'Unreported'
        WHEN age = -1               THEN 'Unreported'
          ELSE NULL END                                                                 AS age_cohort,
      COALESCE(gender_dropdown, gender,'Did Not Identify')                              AS gender,
      COALESCE(ethnicity, 'Did Not Identify')                                           AS ethnicity, 
      country,
      nationality,
      region,
      CASE
        WHEN region = 'Americas' AND country IN ('United States', 'Canada','Mexico') 
          THEN 'NORAM'
        WHEN region = 'Americas' AND country NOT IN ('United States', 'Canada','Mexico') 
          THEN 'LATAM'
        ELSE region END                                                                AS region_modified,
        IFF(country='United States', 
            COALESCE(gender_dropdown, gender,'Did Not Identify')  || '_' || country, 
            COALESCE(gender_dropdown, gender,'Did Not Identify')  || '_'|| 'Non-US')   AS gender_region,
        greenhouse_candidate_id,
        uploaded_at                                                                    AS last_updated_date,
      CASE
        WHEN COALESCE(gender_dropdown, gender,'Did Not Identify') NOT IN ('Male', 'Did Not Identify')
            THEN TRUE
        WHEN COALESCE(ethnicity, 'Did Not Identify') NOT IN ('White','Did Not Identify')
            THEN TRUE
        ELSE FALSE END                                                                  AS urg_group
    FROM source
    WHERE hire_date IS NOT NULL
        OR (LOWER(first_name) NOT LIKE '%greenhouse test%'
            and LOWER(last_name) NOT LIKE '%test profile%'
            and LOWER(last_name) != 'test-gitlab')
        OR employee_id  NOT IN (42039, 42043)


)

SELECT *
FROM intermediate