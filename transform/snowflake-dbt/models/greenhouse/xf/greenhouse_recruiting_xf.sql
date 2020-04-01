WITH applications AS (

    SELECT *
    FROM  {{ ref ('greenhouse_applications') }}

), greenhouse_application_jobs AS (

    SELECT *
    FROM  {{ ref ('greenhouse_applications_jobs') }}

), offers AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_offers') }}

), jobs AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_jobs') }}

), greenhouse_departments AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_departments') }}

), greenhouse_sources AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_sources') }}

), greenhouse_sourcer AS (

    SELECT * 
    FROM {{ ref ('greenhouse_sourcer') }} 

), candidates AS (

    SELECT * 
    FROM {{ ref ('greenhouse_candidates') }}  

), cost_center AS (

    SELECT DISTINCT division, department
    FROM {{ref('cost_center_division_department_mapping')}}

), bamboo AS (

    SELECT greenhouse_candidate_id, hire_date 
    FROM {{ref('bamboohr_id_employee_number_mapping')}}
    WHERE greenhouse_candidate_id IS NOT NULL
)

SELECT 
    applications.application_id, 
    offer_id,
    applications.candidate_id, 
    requisition_id, 
    application_status,
    stage_name                                                                      AS current_stage_name, 
    offer_status,
    applied_at                                                                      AS application_date,
    sent_at                                                                         AS offer_sent_date,
    resolved_at                                                                     AS offer_resolved_date,
    job_name,
    department_name::VARCHAR(100)                                                   AS department_name,
    division::VARCHAR(100)                                                          AS division,                                             
    CASE WHEN lower(department_name) LIKE '%sales%' THEN 'Sales'
         WHEN department_name = 'Dev' THEN 'Engineering'
         WHEN department_name = 'Customer Success Management' THEN 'Sales'
         ELSE COALESCE(cost_center.division, department_name) END::VARCHAR(100)     AS division_modified,     
    greenhouse_sources.source_name::VARCHAR(250)                                    AS source_name,
    greenhouse_sources.source_type::VARCHAR(250)                                    AS source_type,
    greenhouse_sourcer.sourcer_name,
    candidates.candidate_recruiter,
    candidates.candidate_coordinator,
    IFF(greenhouse_sources.source_name ='LinkedIn (Prospecting)',True, False)       AS sourced_candidate,
    IFF(offer_status ='accepted',
            DATEDIFF('day', applications.applied_at, offers.resolved_at),
            NULL)                                                                   AS time_to_offer,
    IFF(bamboo.hire_date IS NOT NULL, TRUE, FALSE)                                  AS is_hired_in_bamboo
FROM applications 
LEFT JOIN greenhouse_application_jobs 
  ON greenhouse_application_jobs.application_id = applications.application_id
LEFT JOIN jobs 
  ON greenhouse_application_jobs.job_id = jobs.job_id
LEFT JOIN  greenhouse_departments 
  ON jobs.department_id = greenhouse_departments.department_id
  AND jobs.organization_id = greenhouse_departments.organization_id
LEFT JOIN greenhouse_sources 
  ON greenhouse_sources.source_id = applications.source_id 
LEFT JOIN offers 
  ON applications.application_id = offers.application_id
LEFT JOIN candidates
  ON applications.candidate_id = candidates.candidate_id  
LEFT JOIN greenhouse_sourcer
  ON applications.application_id = greenhouse_sourcer.application_id
LEFT JOIN cost_center
  ON TRIM(greenhouse_departments.department_name)=TRIM(cost_center.department)
LEFT JOIN bamboo
  ON bamboo.greenhouse_candidate_id = applications.candidate_id  




