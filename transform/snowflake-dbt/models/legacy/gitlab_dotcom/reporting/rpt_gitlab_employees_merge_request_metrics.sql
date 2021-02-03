WITH merge_requests AS (
    
    SELECT *
    FROM {{ ref('gitlab_employees_merge_requests_xf') }}
    WHERE merged_at IS NOT NULL
  
), employees AS (
  
    SELECT *
    FROM {{ref('gitlab_bamboohr_employee_base')}}

), intermediate AS (  

    SELECT
      employees.*,
      merge_requests.merge_request_id, 
      merge_requests.merge_request_data_source,
      merge_requests.merged_at
    FROM employees
    LEFT JOIN MRs
      ON merge_requests.bamboohr_employee_id = employees.employee_id
      AND DATE_TRUNC(day, merge_requests.merged_at) BETWEEN employees.valid_from AND COALESCE(employees.valid_to, '2020-02-28')

)

    SELECT 
      month_date,
      employee_id,
      gitlab_dotcom_user_id,
      division,
      department,
      job_role,
      jobtitle_speciality,
      reports_to,
      total_days,
      COUNT(DISTINCT(merge_request_id)) AS total_merged,
      COUNT(IFF(merge_request_data_source = 'gitlab_dotcom',merge_request_id,NULL)) AS total_gitlab_dotcom_merge_requests,
      COUNT(IFF(merge_request_data_source = 'gitlab_ops',merge_request_id,NULL))    AS total_gitlab_ops_merge_requests
    FROM intermediate
    GROUP BY 1,2,3,4,5,6,7,8


