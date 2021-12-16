{{ simple_cte([
    ('hires','greenhouse_hires'),
    ('applications','greenhouse_applications_source'),
    ('sources','greenhouse_sources_source'),
    ('offers','greenhouse_offers_source'),
    ('openings','greenhouse_openings_source'),
    ('job_departments','greenhouse_jobs_departments_source'),
    ('departments','wk_prep_greenhouse_departments')
])}}

SELECT
  hires.unique_key,
  hires.hire_date_mod               AS hire_date,
  hires.region,
  hires.division,
  hires.department,
  hires.hired_in_bamboohr,
  hires.candidate_id,
  hires.job_opening_type,
  hires.hire_type,
  sources.source_name,
  sources.source_type,
  applications.applied_at,
  openings.job_opened_at            AS job_opening_opened_at,
  openings.job_closed_at            AS job_opening_closed_at,
  openings.target_start_date        AS job_opening_target_start_date,
  offers.sent_at                    AS offer_sent_at,
  offers.resolved_at                AS offer_accepted_at,
  departments.department_name       AS greenhouse_department_name,
  departments.level_1               AS greenhouse_department_level_1,
  departments.level_2               AS greenhouse_department_level_2,
  departments.level_3               AS greenhouse_department_level_3
FROM hires
LEFT JOIN job_departments
  ON hires.job_id = job_departments.job_id
LEFT JOIN departments
  ON job_departments.department_id = departments.department_id
LEFT JOIN applications
  ON hires.application_id = applications.application_id
LEFT JOIN sources
  ON applications.source_id = sources.source_id
LEFT JOIN offers
  ON applications.application_id = offers.application_id
  AND offers.offer_status = 'accepted'
LEFT JOIN openings
  ON hires.application_id = openings.hired_application_id

