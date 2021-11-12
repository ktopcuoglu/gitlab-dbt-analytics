WITH job_openings AS (
    
    SELECT * 
    FROM {{ ref('rpt_greenhouse_current_openings') }}
),
application_jobs AS (
    
    SELECT * 
    FROM {{ ref('greenhouse_applications_jobs_source') }}
),
applications AS (
    
    SELECT * 
    FROM {{ ref('greenhouse_applications_source') }}
),
interviews AS (
    
    SELECT * 
    FROM {{ ref('greenhouse_scheduled_interviews_source') }}
)

  SELECT
    job_openings.job_id,
    job_openings.job_opening_id,
    interviews.application_id,
    interviews.scheduled_interview_id,
    interviews.interview_starts_at,
    interviews.scheduled_interview_stage_name
  FROM job_openings
  LEFT JOIN application_jobs
    ON job_openings.job_id = application_jobs.job_id
  LEFT JOIN applications
    ON application_jobs.application_id = applications.application_id
  INNER JOIN interviews
    ON applications.application_id = interviews.application_id