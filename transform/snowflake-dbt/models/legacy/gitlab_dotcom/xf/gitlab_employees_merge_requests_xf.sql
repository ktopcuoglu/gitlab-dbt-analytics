WITH gitlab_dotcom_merge_requests AS (

    SELECT * 
    FROM {{ref('gitlab_dotcom_merge_requests_xf')}} 
    WHERE is_part_of_product = TRUE

), gitlab_ops_merge_requests AS (

    SELECT * 
    FROM {{ref('gitlab_ops_merge_requests_xf')}} 
    WHERE is_part_of_product_ops = TRUE

), mapped_employee AS (

    SELECT *
    FROM {{ref('map_team_member_bamboo_gitlab_dotcom_gitlab_ops')}} 

), employee_directory AS (

    SELECT *
    FROM {{ref('employee_directory_analysis')}} 

) joined AS (

    SELECT 
      'gitlab_dotcom'                           AS data_source,
      gitlab_dotcom_merge_requests.*,
      mapped_employee.bamboo_hr_employee_id,
      employee_directory.division,
      employee_directory.department
    FROM gitlab_dotcom_merge_requests
    LEFT JOIN mapped_employee
      ON gitlab_dotcom_merge_requests.author_id = mapped_employee.gitlab_dotcom_user_id
    LEFT JOIN employee_directory
      ON mapped_employee.bamboo_hr_employee_id = employee_directory.employee_id
      AND DATE_TRUNC(day, gitlab_dotcom_merge_requests.merged_at) = employee_directory.date_actual

     UNION ALL

    SELECT 
      'gitlab_ops'                           AS data_source,
      gitlab_dotcom_merge_requests.*,
      mapped_employee.bamboo_hr_employee_id,
      employee_directory.division,
      employee_directory.department
    FROM gitlab_ops_merge_requests
    LEFT JOIN mapped_employee
      ON gitlab_ops_merge_requests.author_id = mapped_employee.gitlab_dotcom_user_id
    LEFT JOIN employee_directory
      ON mapped_employee.bamboo_hr_employee_id = employee_directory.employee_id
      AND DATE_TRUNC(day, gitlab_ops_merge_requests.updated_at) = employee_directory.date_actual ---using updated_at until we get merged_at date

)

SELECT *
FROM joined 

