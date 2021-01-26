WITH applications AS (

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY applied_at)     AS greenhouse_candidate_row_number
    FROM  {{ ref ('greenhouse_applications_source') }}
    WHERE application_status = 'hired'

), offers AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_offers_source') }}
    WHERE offer_status = 'accepted'

), openings AS (
    
    SELECT *
    FROM {{ ref ('greenhouse_openings_source') }}

), greenhouse_opening_custom_fields AS (

    SELECT *
    FROM  {{ ref ('greenhouse_opening_custom_fields') }}

), initial_hire_date AS (
 
    SELECT *
    FROM {{ ref ('bamboohr_employment_status_xf') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY valid_from_date) = 1

), rehire_date AS (
 
    SELECT *
    FROM {{ ref ('bamboohr_employment_status_xf') }}
    WHERE is_rehire = 'True'

 ), bamboohr_mapping AS (
 
    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping') }}
   
), bamboo_hires AS (

    SELECT 
      bamboohr_mapping.employee_id,
      bamboohr_mapping.greenhouse_candidate_id,
      CONCAT(bamboohr_mapping.first_name, ' ', bamboohr_mapping.last_name)      AS employee_name,
      COALESCE(initial_hire_date.valid_from_date, bamboohr_mapping.hire_date)   AS initial_hire_date,
      bamboohr_mapping.region,
      rehire_date.valid_from_date                                               AS rehire_date
    FROM bamboohr_mapping
    LEFT JOIN initial_hire_date 
      ON initial_hire_date.employee_id = bamboohr_mapping.employee_id
    LEFT JOIN rehire_date 
      ON rehire_date.employee_id = bamboohr_mapping.employee_id

), division_department AS (

    SELECT *
    FROM {{ ref ('employee_directory_intermediate') }}
    
), joined AS (

    SELECT 
      applications.application_id,  
      applications.candidate_id, 
      bamboo_hires.employee_id,
      bamboo_hires.employee_name,
      offers.start_date                                             AS candidate_target_hire_date, 
      applications.applied_at, 
      bamboo_hires.region,
      applications.greenhouse_candidate_row_number,
      IFF(applications.greenhouse_candidate_row_number = 1 
            AND applied_at < initial_hire_date, 
              initial_hire_date, candidate_target_hire_date)        AS hire_date_mod,
      CASE WHEN greenhouse_candidate_row_number = 1 
            THEN 'hire'
           WHEN offers.start_date = bamboo_hires.rehire_date
            THEN 'rehire'
           WHEN greenhouse_candidate_row_number>1 
            THEN 'transfer'
           ELSE NULL END                                            AS hire_type,
      greenhouse_opening_custom_fields.job_opening_type,
      division_department.division,
      division_department.department
    FROM applications
    LEFT JOIN offers
      ON offers.application_id = applications.application_id
    LEFT JOIN bamboo_hires 
      ON bamboo_hires.greenhouse_candidate_id = applications.candidate_id
    LEFT JOIN openings
      ON openings.hired_application_id = applications.application_id
    LEFT JOIN greenhouse_opening_custom_fields
      ON greenhouse_opening_custom_fields.job_opening_id = openings.job_opening_id
    LEFT JOIN division_department
      ON division_department.employee_id = bamboo_hires.employee_id
      AND division_department.date_actual =  IFF(applications.greenhouse_candidate_row_number = 1 
            AND applied_at < initial_hire_date, 
              initial_hire_date, offers.start_date)

), final AS (    

    SELECT 
      {{ dbt_utils.surrogate_key(['application_id', 'candidate_id',]) }}  AS unique_key,
      application_id,
      candidate_id,
      employee_id,
      employee_name,
      region,
      greenhouse_candidate_row_number,
      hire_date_mod,
      hire_type,
      job_opening_type,
      IFF(employee_id IS NOT NULL,TRUE,FALSE)                             AS hired_in_bamboohr
    FROM joined 

)

SELECT *
FROM final