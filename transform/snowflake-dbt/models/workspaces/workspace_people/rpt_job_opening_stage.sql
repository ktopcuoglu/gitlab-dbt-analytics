{{ simple_cte([
    ('job_openings','rpt_greenhouse_current_openings'),
    ('application_jobs','greenhouse_applications_jobs_source'),
    ('applications','greenhouse_applications_source'),
    ('application_stages','greenhouse_application_stages_source'),
    ('sources','greenhouse_sources_source'),
    ('job_departments','greenhouse_jobs_departments_source'),
    ('departments','wk_prep_greenhouse_departments')
])}}

  SELECT
    job_openings.job_id,
    job_openings.job_opening_id,
    application_stages.stage_entered_on          AS stage_entered_date,
    application_stages.stage_exited_on           AS stage_exited_date,
    application_stages.application_stage_name,
    application_stages.application_id,
    sources.source_name,
    sources.source_type,
    departments.department_name                  AS greenhouse_department_name,
    departments.level_1                          AS greenhouse_department_level_1,
    departments.level_2                          AS greenhouse_department_level_2,
    departments.level_3                          AS greenhouse_department_level_3
  FROM job_openings
  LEFT JOIN application_jobs
    ON job_openings.job_id = application_jobs.job_id
  LEFT JOIN applications
    ON application_jobs.application_id = applications.application_id
  LEFT JOIN application_stages
    ON application_jobs.application_id = application_stages.application_id 
    AND application_stages.stage_entered_on IS NOT NULL
  LEFT JOIN sources
    ON applications.source_id = sources.source_id
  LEFT JOIN job_departments
    ON job_openings.job_id = job_departments.job_id
  LEFT JOIN departments
    ON job_departments.department_id = departments.department_id
