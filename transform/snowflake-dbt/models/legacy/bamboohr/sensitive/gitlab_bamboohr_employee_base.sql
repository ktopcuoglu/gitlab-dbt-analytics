WITH employee_directory AS (
  
    SELECT *
    FROM {{ ref('employee_directory_intermediate') }}
    WHERE employment_status NOT LIKE '%Leave%'

),  gitlab_mapping AS (

    SELECT *
    FROM {{ ref('map_team_member_bamboo_gitlab_dotcom_gitlab_ops') }}

), sheetload_missing AS (

    SELECT *
    FROM {{ ref('sheetload_infrastructure_missing_employees') }}

), intermediate AS (

    SELECT DISTINCT
        DATE_TRUNC(month, date_actual) AS month_date,
        employee_directory.date_actual AS valid_from,
        employee_directory.employee_id,
        employee_directory.full_name,
        division,
        department,
        jobtitle_speciality,
        job_role,
        reports_to,
        gitlab_dotcom_user_id,
        gitlab_ops_user_id
    FROM employee_directory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY
                                DATE_TRUNC(month, date_actual), employee_directory.employee_id, 
                                employee_directory.division, employee_directory.department, jobtitle_speciality, 
                                job_role_modified, reports_to 
                                ORDER BY employee_directory.date_actual) = 1

), final AS (

    SELECT 
      month_date,
      valid_from,
      LEAD(DATEADD(day, -1, valid_from)) OVER (PARTITION BY employee_id order by valid_from) AS valid_to,
      employee_id,
      full_name,
      division,
      department,
      jobtitle_speciality,
      reports_to,
      gitlab_dotcom_user_id,
      gitlab_ops_user_id,
      DATEDIFF(day, valid_from, valid_to)      AS total_days
    FROM intermediate
    -- need to account for terminations

)

SELECT *
FROM final
