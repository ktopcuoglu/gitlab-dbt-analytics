WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT 
      NULLIF(d.value['employeeNumber'],'')::NUMBER                    AS employee_number,
      d.value['id']::NUMBER                                           AS employee_id,
      d.value['firstName']::VARCHAR                                   AS first_name,
      d.value['lastName']::VARCHAR                                    AS last_name,
      (CASE WHEN d.value['hireDate']=''
            THEN NULL
           WHEN d.value['hireDate']= '0000-00-00'
            THEN NULL
           ELSE d.value['hireDate']::VARCHAR END)::DATE               AS hire_date,
      (CASE WHEN d.value['terminationDate']=''
            THEN NULL
           WHEN d.value['terminationDate']= '0000-00-00'
            THEN NULL
           ELSE d.value['terminationDate']::VARCHAR END)::DATE        AS termination_date,
      d.value['customNationality']::VARCHAR                           AS nationality,
      d.value['customRegion']::VARCHAR                                AS region,
      d.value['ethnicity']::VARCHAR                                   AS ethnicity,
      d.value['gender']::VARCHAR                                      AS gender, 
      d.value['customOtherGenderOptions']::VARCHAR                    AS gender_dropdown, 
      TRIM(d.value['country']::VARCHAR)                               AS country,
      d.value['age']::NUMBER                                          AS age,
      d.value['customCandidateID']::NUMBER                            AS greenhouse_candidate_id
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['employees']), outer => true) d

), final AS (

    SELECT
      employee_number,
      employee_id,
      first_name,
      last_name,
      hire_date,
      termination_date,
      CASE WHEN age BETWEEN 18 AND 24 THEN '18-24'
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
          ELSE NULL END                                       AS age_cohort,
      country,
      ethnicity,
      COALESCE(gender_dropdown, gender,'Did Not Identify')    AS gender,
      nationality,
      region,
      CASE WHEN region = 'Americas' AND country IN ('United States', 'Canada','Mexico') 
            THEN 'NORAM'
         WHEN region = 'Americas' AND country NOT IN ('United States', 'Canada','Mexico') 
            THEN 'LATAM'
         ELSE region END                                        AS region_modified,
      IFF(country='United States', COALESCE(gender_dropdown, gender,'Did Not Identify')  || '_' || country, 
                                   COALESCE(gender_dropdown, gender,'Did Not Identify')  || '_'|| 'Non-US') AS gender_region,
      greenhouse_candidate_id
    FROM intermediate
    WHERE hire_date IS NOT NULL
        AND (LOWER(first_name) NOT LIKE '%greenhouse test%'
            and LOWER(last_name) NOT LIKE '%test profile%'
            and LOWER(last_name) != 'test-gitlab')
        AND employee_id  NOT IN (42039, 42043)

)

SELECT *
FROM final
