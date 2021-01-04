WITH source AS (
    
    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping_source') }}
  
), intermediate AS (

    SELECT 
      {{ dbt_utils.surrogate_key(['employee_id', 'job_role', 'job_grade', 
                                  'cost_center', 'jobtitle_speciality', 
                                  'gitlab_username', 'pay_frequency', 
                                  'sales_geo_differential']) }}        AS unique_key,
      employee_number,
      employee_id,
      first_name,
      last_name,
      hire_date,
      termination_date,
      job_role,
      job_grade,
      cost_center,
      jobtitle_speciality,
      gitlab_username,
      pay_frequency,
      sales_geo_differential,
      DATE_TRUNC(day, uploaded_at)                                    AS effective_date   
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key
            ORDER BY DATE_TRUNC(day,effective_date) ASC, DATE_TRUNC(hour, effective_date) DESC)=1  

), final AS (

    SELECT *,
      LEAD(DATEADD(day,-1,DATE_TRUNC(day, intermediate.effective_date))) OVER 
        (PARTITION BY employee_number ORDER BY intermediate.effective_date)              AS next_effective_date
    FROM intermediate 
    WHERE effective_date>= '2020-02-27'  --1st day we started capturing job role

) 

SELECT * 
FROM final
