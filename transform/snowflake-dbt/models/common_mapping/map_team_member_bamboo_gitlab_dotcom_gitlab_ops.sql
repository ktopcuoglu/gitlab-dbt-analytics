WITH bamboo_hr_members AS (

    SELECT DISTINCT 
      employee_id, 
      full_Name, 
      work_email
    FROM legacy.employee_directory_analysis 
    WHERE work_email IS NOT NULL 

), gitlab_dotcom_members AS (

    SELECT * 
    FROM common.dim_gitlab_dotcom_gitlab_emails  
  
), gitlab_ops_members AS (

    SELECT
      user_id                   AS gitlab_ops_user_id,
      gitlab_ops_user_name, 
      notification_email        AS gitlab_ops_email_address
    FROM common.dim_gitlab_ops_gitlab_emails
  
), final AS (

    SELECT 
      full_name                 AS bamboo_hr_full_name, 
      work_email                AS bamboo_hr_gitlab_email, 
      employee_id               AS bamboo_hr_employee_id, 
      gitlab_dotcom_user_id, 
      gitlab_dotcom_user_name, 
      gitlab_ops_user_id, 
      gitlab_ops_user_name
    FROM bamboo_hr_members
    LEFT JOIN gitlab_dotcom_members
        ON bamboo_hr_members.work_email = gitlab_dotcom_members.gitlab_dotcom_email_address 
    LEFT JOIN gitlab_ops_members
        ON bamboo_hr_members.work_email = gitlab_ops_members.gitlab_ops_email_address 

) 

SELECT * 
FROM final 

