{{ simple_cte([
    ('job_openings','rpt_greenhouse_current_openings'),
    ('application_jobs','greenhouse_applications_jobs_source'),
    ('application_stages','greenhouse_application_stages_source')
])}}

  SELECT
    job_openings.job_id,
    job_openings.job_opening_id,
    application_stages.stage_entered_on          AS stage_entered_date,
    application_stages.stage_exited_on           AS stage_exited_date,
    application_stages.application_stage_name,
    application_stages.application_id
  FROM job_openings
  LEFT JOIN application_jobs
    ON job_openings.job_id = application_jobs.job_id
  LEFT JOIN application_stages
    ON application_jobs.application_id = application_stages.application_id 
    AND application_stages.stage_entered_on IS NOT NULL
