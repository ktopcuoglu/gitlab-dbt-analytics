WITH source AS (
    
    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping_source') }}
  
), intermediate AS (

    SELECT 
      {{ dbt_utils.surrogate_key(['employee_id', 'job_role', 'job_grade', 
                                  'cost_center', 'jobtitle_speciality', 
                                  'gitlab_username', 'pay_frequency', 
                                  'sales_geo_differential']) }}        AS unique_key
      employee_number,
      employee_id,
      first_name,
      last_name,
      hire_date,
      job_role,
      job_grade,
      cost_center,
      jobtitle_speciality,
      gitlab_username,
      pay_frequency,
      sales_geo_differential,
      DATE_TRUNC(day, uploaded_at)                                    AS effective_date


      {# NULLIF(d.value['employeeNumber'],'')::NUMBER                    AS employee_number,
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
      d.value['customPayFrequency']::VARCHAR                          AS pay_frequency,
      d.value['customSalesGeoDifferential']::VARCHAR                  AS sales_geo_differential,
      uploaded_at::TIMESTAMP                                          AS effective_date, #}      
    FROM source,
    {# LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['employees']), OUTER => true) d #}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key
            ORDER BY DATE_TRUNC(day,effective_date) ASC, DATE_TRUNC(hour, effective_date) DESC)=1  

), final AS (

    SELECT *,
      LEAD(DATEADD(day,-1,DATE_TRUNC(day, intermediate.effective_date))) OVER 
        (PARTITION BY employee_number ORDER BY intermediate.effective_date)              AS next_effective_date
    FROM intermediate 
    WHERE effective_date>= '2020-02-27'  --1st day we started capturing job role
      AND hire_date IS NOT NULL
      AND (LOWER(first_name) NOT LIKE '%greenhouse test%'
      AND LOWER(last_name) NOT LIKE '%test profile%'
      AND LOWER(last_name) != 'test-gitlab')
      AND employee_id != 42039

) 

SELECT * 
FROM final
