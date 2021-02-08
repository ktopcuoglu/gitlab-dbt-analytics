{% set lines_to_repeat %}
      month_date,
      COUNT(DISTINCT(intermediate.employee_id))                                             AS total_employees,
      COUNT(DISTINCT(IFF(is_part_of_product = TRUE, intermediate.employee_id, NULL)))       AS total_employees_with_mr_part_of_product,
      COUNT(IFF(is_part_of_product = TRUE, merge_request_id, NULL))                         AS total_merged_part_of_product,
      COUNT(IFF(is_part_of_product = TRUE AND 
                merge_request_data_source = 'gitlab_dotcom',merge_request_id,NULL))         AS total_gitlab_dotcom_product_merge_requests,
      COUNT(IFF(is_part_of_product = TRUE 
                AND merge_request_data_source = 'gitlab_ops',merge_request_id,NULL))        AS total_gitlab_ops_product_merge_requests,
      ROUND((total_merged_part_of_product/total_employees),2)                               AS narrow_mr_rate,
      total_employees_with_mr_part_of_product/total_employees                               AS percent_of_employees_with_mr,
      SUM(people_engineering_project)                                                       AS total_people_engineering_merge_requests
    FROM intermediate
{% endset %}

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
      merge_requests.merged_at,
      merge_requests.is_part_of_product,
      people_engineering_project
    FROM employees
    LEFT JOIN merge_requests
      ON merge_requests.bamboohr_employee_id = employees.employee_id
      AND DATE_TRUNC(day, merge_requests.merged_at) BETWEEN employees.valid_from AND COALESCE(employees.valid_to, '2020-02-28')

), aggregated AS (

    SELECT
      'division'                                                                           AS breakout_level,
      division,
      NULL                                                                                 AS department,
      NULL                                                                                 AS employee_id,
      {{lines_to_repeat}}
      {{ dbt_utils.group_by(n=5) }}  

    UNION ALL

    SELECT
      'department'                                                                          AS breakout_level,
      division,
      department,
      NULL                                                                                  AS employee_id,
      {{lines_to_repeat}}
      {{ dbt_utils.group_by(n=5) }}  

    UNION ALL

    SELECT
      'team_member'                                                                        AS breakout_level,
      division,
      department,
      employee_id,
      {{lines_to_repeat}}
      {{ dbt_utils.group_by(n=5) }}  

    UNION ALL

    SELECT
      'division_modified'                                                                  AS breakout_level,
      'R&D_engineering_and_product'                                                        AS division,
      NULL                                                                                 AS department,
      NULL                                                                                 AS employee_id,
      {{lines_to_repeat}}
      WHERE division IN ('Engineering','Product')
        AND department != 'Customer Support'
      {{ dbt_utils.group_by(n=5) }}  

)

SELECT *
FROM aggregated