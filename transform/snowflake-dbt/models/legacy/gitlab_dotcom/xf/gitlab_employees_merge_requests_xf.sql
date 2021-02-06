{{ simple_cte([
    ('gitlab_dotcom_merge_requests','map_merged_crm_accounts'),
    ('gitlab_dotcom_merge_request_metrics','gitlab_dotcom_merge_request_metrics'),
    ('gitlab_ops_merge_requests','gitlab_ops_merge_requests'),
    ('gitlab_ops_merge_request_metrics','gitlab_ops_merge_request_metrics'),
    ('mapped_employees','map_team_member_bamboo_gitlab_dotcom_gitlab_ops'),
    ('employee_directory','employee_directory_analysis')
    
]) }}

, joined AS (

    SELECT
      'gitlab_dotcom'                           AS merge_request_data_source,
      {{ dbt_utils.star(from=ref('gitlab_ops_merge_requests'), except=["created_at", "updated_at"]) }},
      created_at                                                                           AS merge_request_created_at,
      updated_at                                                                           AS merge_request_updated_at,
      gitlab_dotcom_merge_requests.*,
      gitlab_dotcom_merge_request_metrics.merged_at,
      IFF(merge_requests.target_project_id IN ({{is_project_included_in_engineering_metrics()}}),
        TRUE, FALSE)                                                                       AS is_included_in_engineering_metrics,
      IFF(merge_requests.target_project_id IN ({{is_project_part_of_product()}}),
        TRUE, FALSE)                                                                       AS is_part_of_product,
      mapped_employee.bamboohr_employee_id,
      employee_directory.division,
      employee_directory.department
    FROM gitlab_dotcom_merge_requests
    LEFT JOIN gitlab_dotcom_merge_request_metrics
        ON gitlab_dotcom_merge_request_metrics.merge_request_id = gitlab_dotcom_merge_request_metrics.merge_request_id
        AND gitlab_dotcom_merge_request_metrics.merged_at IS NOT NULL
    INNER JOIN mapped_employee
      ON gitlab_dotcom_merge_requests.author_id = mapped_employee.gitlab_dotcom_user_id
    LEFT JOIN employee_directory
      ON mapped_employee.bamboohr_employee_id = employee_directory.employee_id
      AND DATE_TRUNC(day, gitlab_dotcom_merge_requests.merged_at) = employee_directory.date_actual
)

SELECT *
FROM joined 

