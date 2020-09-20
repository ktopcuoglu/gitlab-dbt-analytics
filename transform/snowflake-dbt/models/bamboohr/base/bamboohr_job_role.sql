WITH source AS (
    
    SELECT *
    FROM {{ source('bamboohr', 'id_employee_number_mapping') }} 
  
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
      d.value['customRole']::VARCHAR                                  AS job_role,
      d.value['customJobGrade']::VARCHAR                              AS job_grade,
      d.value['customCostCenter']::VARCHAR                            AS cost_center,
      d.value['customJobTitleSpeciality']::VARCHAR                    AS jobtitle_speciality,
      d.value['customGitLabUsername']::VARCHAR                        AS gitlab_username,
      d.value['customSalesGeoDifferential']::VARCHAR                  AS sales_geo_differential,
      uploaded_at::TIMESTAMP                                          AS effective_date
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['employees']), OUTER => true) d
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, job_role, job_grade, cost_center,
                                            jobtitle_speciality, gitlab_username, sales_geo_differential
            ORDER BY DATE_TRUNC(day,effective_date) ASC, DATE_TRUNC(hour, effective_date) DESC)=1  


), engineering_sheetload_speciality AS (

     SELECT 
      employee_id, 
      speciality, 
      start_date                                                                           AS speciality_start_date,
      LEAD(DATEADD(day,-1,start_date)) OVER (PARTITION BY employee_id ORDER BY start_date) AS speciality_end_date
    FROM {{ ref ('sheetload_engineering_speciality_prior_to_capture') }}

), final AS (

    SELECT 
      intermediate.employee_number,
      intermediate.employee_id,
      intermediate.job_role,
      intermediate.job_grade,
      intermediate.cost_center,
      COALESCE(engineering_sheetload_speciality.speciality, intermediate.jobtitle_speciality)         AS jobtitle_speciality,
      intermediate.gitlab_username,
      intermediate.sales_geo_differential,
      DATE_TRUNC(day, intermediate.effective_date)                                                    AS effective_date,
      LEAD(DATEADD(day,-1,DATE_TRUNC(day, intermediate.effective_date))) OVER 
        (PARTITION BY intermediate.employee_number ORDER BY intermediate.effective_date)              AS next_effective_date
    FROM intermediate 
    LEFT JOIN engineering_sheetload_speciality
      ON intermediate.employee_id = engineering_sheetload_speciality.employee_id
      AND intermediate.effective_date BETWEEN engineering_sheetload_speciality.speciality_start_date 
                                  AND COALESCE(engineering_sheetload_speciality.speciality_end_date, '2020-09-30') --transistioning changes in speciality to bamboohr
    WHERE intermediate.effective_date>= '2020-02-27'  --1st day we started capturing job role
      AND intermediate.hire_date IS NOT NULL
      AND (LOWER(intermediate.first_name) NOT LIKE '%greenhouse test%'
      AND LOWER(intermediate.last_name) NOT LIKE '%test profile%'
      AND LOWER(intermediate.last_name) != 'test-gitlab')
      AND intermediate.employee_id != 42039

) 

SELECT * 
FROM final
