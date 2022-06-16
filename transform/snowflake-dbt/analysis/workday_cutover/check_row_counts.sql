{{ config(
  enabled=false
) }}



select
  'bamboohr_directionary_bonuses_xf' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_directionary_bonuses_xf') }} --pempey_prep.sensitive.bamboohr_directionary_bonuses_xf
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_directionary_bonuses_xf') }} ) AS new

UNION

select
  'bamboohr_missing_emergency_contact_alert' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_missing_emergency_contact_alert') }} --pempey_prod.legacy.bamboohr_missing_emergency_contact_alert
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_missing_emergency_contact_alert') }} ) AS new

UNION
select
  'bamboohr_employment_status_xf' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_employment_status_xf') }} --pempey_prep.sensitive.bamboohr_employment_status_xf
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_employment_status_xf') }} ) AS new

UNION
select
  'bamboohr_id_employee_number_mapping' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_id_employee_number_mapping') }} --pempey_prep.sensitive.bamboohr_id_employee_number_mapping
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_id_employee_number_mapping') }} ) AS new

UNION
select
  'bamboohr_job_role' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_job_role') }} --pempey_prep.sensitive.bamboohr_job_role
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_job_role') }} ) AS new

UNION
select
  'bamboohr_promotions_xf' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_promotions_xf') }} --pempey_prep.sensitive.bamboohr_promotions_xf
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_promotions_xf') }} ) AS new

UNION
select
  'bamboohr_separations' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_separations') }} --pempey_prep.sensitive.bamboohr_separations
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_separations') }} ) AS new

UNION
select
  'employee_directory' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('employee_directory') }} --pempey_prep.sensitive.employee_directory
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_employee_directory') }} ) AS new

UNION
select
  'employee_directory_intermediate' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('employee_directory_intermediate') }} --pempey_prep.sensitive.employee_directory_intermediate
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_employee_directory_intermediate') }} ) AS new

UNION
select
  'employee_locality' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('employee_locality') }} --pempey_prep.sensitive.employee_locality
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_employee_locality') }} ) AS new

UNION
select
  'bamboohr_discretionary_bonuses' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_discretionary_bonuses') }} --pempey_prod.legacy.bamboohr_discretionary_bonuses
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_discretionary_bonuses') }} ) AS new

UNION
select
  'bamboohr_job_info_current_division_base' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_job_info_current_division_base') }} --pempey_prod.legacy.bamboohr_job_info_current_division_base
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_job_info_current_division_base') }} ) AS new

UNION
select
  'bamboohr_work_email' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('bamboohr_work_email') }} --pempey_prod.legacy.bamboohr_work_email
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_bamboohr_work_email') }} ) AS new

UNION
select
  'rpt_greenhouse_hired_employees_opening_ids' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('rpt_greenhouse_hired_employees_opening_ids') }} --pempey_prod.legacy.rpt_greenhouse_hired_employees_opening_ids
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_rpt_greenhouse_hired_employees_opening_ids') }} ) AS new

UNION
select
  'employee_directory_analysis' AS report_table,
  count(*) AS old_row_count,
  min(new.row_count) AS new_row_count
from {{ ref('employee_directory_analysis') }} --pempey_prod.legacy.employee_directory_analysis
LEFT JOIN (select count(*) AS row_count from {{ ref('workday_employee_directory_analysis') }} ) AS new