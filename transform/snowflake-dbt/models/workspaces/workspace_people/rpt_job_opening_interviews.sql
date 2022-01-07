

{{ simple_cte([
    ('job_openings','rpt_greenhouse_current_openings'),
    ('application_jobs','greenhouse_applications_jobs_source'),
    ('applications','greenhouse_applications_source'),
    ('sources','greenhouse_sources_source'),
    ('interviews','greenhouse_scheduled_interviews_source'),
    ('job_departments','greenhouse_jobs_departments_source'),
    ('departments','wk_prep_greenhouse_departments')
])}}

  SELECT
    job_openings.job_id,
    job_openings.job_opening_id,
    interviews.application_id,
    interviews.scheduled_interview_id,
    interviews.interview_starts_at,
    interviews.scheduled_interview_stage_name,
    sources.source_name,
    sources.source_type,
    departments.department_name                 AS greenhouse_department_name,
    departments.level_1                         AS greenhouse_department_level_1,
    departments.level_2                         AS greenhouse_department_level_2,
    departments.level_3                         AS greenhouse_department_level_3
  FROM job_openings
  LEFT JOIN application_jobs
    ON job_openings.job_id = application_jobs.job_id
  LEFT JOIN applications
    ON application_jobs.application_id = applications.application_id
  LEFT JOIN sources
    ON applications.source_id = sources.source_id
  INNER JOIN interviews
    ON applications.application_id = interviews.application_id
  LEFT JOIN job_departments
    ON job_openings.job_id = job_departments.job_id
  LEFT JOIN departments
    ON job_departments.department_id = departments.department_id
  