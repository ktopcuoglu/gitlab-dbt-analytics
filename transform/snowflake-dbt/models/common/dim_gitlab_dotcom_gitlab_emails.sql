WITH gitlab_dotcom_members AS (

    SELECT * 
    FROM {{ref('gitlab_dotcom_members')}} 

), gitlab_dotcom_users_xf AS (

    SELECT * 
    FROM {{ref('gitlab_dotcom_users_xf')}} 

), gitlab_dotcom_gitlab_emails_cleaned AS (

    SELECT DISTINCT 
      user_id, 
      email_address, 
      email_handle, 
    FROM {{ref('gitlab_dotcom_gitlab_emails')}} 
    WHERE length (email_handle) > 3
      AND include_email_flg = 'Include'

), sheetload_infrastructure_gitlab_employee AS (

    SELECT * 
    FROM {{ref('sheetload_infrastructure_missing_employees')}}

), gitlab_dotcom_team_members_user_id AS (

    -- This CTE returns the user_id for any team member in the GitLab.com or GitLab.org project 
    SELECT DISTINCT user_id                                    AS gitlab_dotcom_user_id
    FROM gitlab_dotcom_members
    WHERE is_currently_valid = TRUE 
      AND member_source_type = 'Namespace'
      AND source_id IN (9970,6543) -- 9970 = gitlab-org, 6543 = gitlab-com

), gitlab_dotcom_notification_emails AS (

    -- This CTE cleans and maps GitLab.com user_name and emails for most GitLab team members 
    -- The email field here is notification_email 
    SELECT DISTINCT
    gitlab_dotcom_user_id, 
    user_name,
    CASE 
      WHEN length (notification_email) < 3                  THEN NULL   -- removes records with just one number  
      WHEN notification_email LIKE '%-%'                    THEN NULL   -- removes any emails with special character - 
      WHEN notification_email LIKE '%~%'                    THEN NULL   -- removes admin accounts 
      WHEN notification_email LIKE '%+%'                    THEN NULL   -- removes any emails with special character + 
      WHEN notification_email LIKE '%admin%'                THEN NULL   -- removes records with the word admin
      WHEN notification_email LIKE '%hack%'                 THEN NULL   -- removes hack accounts
      WHEN notification_email LIKE '%xxx%'                  THEN NULL   -- removes accounts with more than three xs
      WHEN notification_email LIKE '%gitlab%@gitlab.com'    THEN NULL   -- removes accounts that have the word gitlab
      WHEN notification_email LIKE '%test%'                 THEN NULL   -- removes accounts with more than three xs
      WHEN notification_email NOT LIKE '%@gitlab.com'       THEN NULL  
    ELSE notification_email END                                AS notification_email
    FROM gitlab_dotcom_team_members_user_id
    INNER JOIN gitlab_dotcom_users_xf AS users
      ON gitlab_dotcom_team_members_user_id.gitlab_dotcom_user_id = users.user_id
    WHERE user_name NOT ILIKE '%admin%'
  
), supplement_notification_emails_with_additional_gitlab_emails AS (

    -- This CTE cleans and maps supplemental GitLab.com email addresses from the `emails` table in gitlab_dotcom
    SELECT 
      gitlab_dotcom_user_id, 
      user_name                                                AS gitlab_dotcom_user_name,
      COALESCE(notification_email,email_address)               AS gitlab_dotcom_email_address
    FROM gitlab_dotcom_notification_emails
    LEFT JOIN gitlab_dotcom_gitlab_emails_cleaned 
      ON gitlab_dotcom_notification_emails.gitlab_dotcom_user_id = gitlab_dotcom_gitlab_emails_cleaned.user_id 

), all_known_employee_gitlab_emails AS (

    -- This CTE cleans and maps supplemental GitLab.com email address from the `sheetload` source  
    SELECT 
      supplement_notification_emails_with_additional_gitlab_emails.gitlab_dotcom_user_id, 
      gitlab_dotcom_user_name, 
      COALESCE(work_email, gitlab_dotcom_email_address)        AS gitlab_dotcom_email_address
    FROM supplement_notification_emails_with_additional_gitlab_emails
    LEFT JOIN sheetload_infrastructure_gitlab_employee
      ON supplement_notification_emails_with_additional_gitlab_emails.gitlab_dotcom_user_id = sheetload_infrastructure_gitlab_employee.gitlab_dotcom_user_id

) 

SELECT * 
FROM all_known_employee_gitlab_emails

