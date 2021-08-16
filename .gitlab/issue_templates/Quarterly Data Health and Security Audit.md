# Quarterly Data Health and Security Audit

Quarterly audit is performed to validate security like right people with right access in environments (Example: Sisense, Snowflake.etc) and data feeds that are running are healthy (Example: Salesforce, GitLab.com..etc).

Please see the [handbook page](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/) for more information. 

Below checklist of activities would be run once for quarter to validate security and system health.

SNOWFLAKE
1. [ ] Validate terminated employees have been removed from Snowflake access.
    <details>

    Cross check between BambooHR and Snowflake
    * [ ] If applicable, check if users set to disabled in Snowflake
    * [ ] If applicable, check if users in [roles.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/permissions/snowflake/roles.yml):
        * [ ] isn't assigned to `warehouses`
        * [ ] isn't assigned to `roles`
        * [ ] can_login set to: `no`

    ```sql

    SELECT DISTINCT 
      employee.employee_id, 
      employee.first_name, 
      employee.last_name, 
      bamboohr.employment_status,
      employee.hire_date, 
      employee.rehire_date, 
      snowflake.last_success_login, 
      snowflake.created_on, 
      employee.termination_date, 
      snowflake.is_disabled 
    FROM "PREP"."BAMBOOHR"."BAMBOOHR_EMPLOYMENT_STATUS_SOURCE" bamboohr
    INNER JOIN PREP."SENSITIVE"."EMPLOYEE_DIRECTORY" employee
    INNER JOIN PROD."LEGACY"."SNOWFLAKE_SHOW_USERS" snowflake
      ON bamboohr.EMPLOYEE_ID = employee.EMPLOYEE_ID 
      AND employee.FIRST_NAME = snowflake.FIRST_NAME 
      AND employee.LAST_NAME = snowflake.LAST_NAME 
      WHERE bamboohr.employment_status = 'Terminated' 
      AND snowflake.is_disabled ='false' 
      AND (CASE WHEN snowflake.last_success_login IS NULL THEN snowflake.created_on ELSE snowflake.last_success_login END) >= employee.termination_date ;

    ```

2. [ ] De-activate any account that has not logged-in within the past 30 days from the moment of performing audit from Snowflake.
    <details>

    ```sql
    SELECT
      user_name,
      created_on,
      login_name,
      display_name,
      first_name,
      last_name,
      email,
      comment,
      is_disabled,
      last_success_login,
      snapshot_date
    FROM "PROD"."LEGACY"."SNOWFLAKE_SHOW_USERS"
    WHERE is_disabled = 'false'
      and CASE WHEN last_success_login IS null THEN created_on ELSE last_success_login END <= dateadd('day', -30, CURRENT_DATE()) ;
    
    ```
  

3. [ ] Validate all user accounts require multi-factor authentication.
    <details>

    * [ ] Check EXT_AUTHN_DUO is set to ‘false’ in users table. If set to ‘false’ then MFA is diabled.

SISENSE
1. [ ] Validate terminated employees have been removed from Sisense access.
    <details>

     ```sql
     
    SELECT   
      sisense.id,  
      employee.full_name, 
      sisense.email_address , 
      employee.is_termination_date 
   FROM  legacy.employee_directory_analysis employee 
   INNER JOIN legacy.sheetload_sisense_users sisense 
    ON  employee.full_name = concat(sisense.first_name,' ', sisense.last_name) 
    AND employee.work_email = sisense.email_address  
    AND employee.is_termination_date = 'TRUE' 
   GROUP BY 1,2,3,4 ;
    
     ```


2. [ ] De-activate any account that has not logged-in within the past 30 days from the moment of performing audit from Sisense.

    <details>

     ```sql


    WITH FINAL AS (
       SELECT
          time_on_site_logs.user_id,
          users.first_name,
          users.last_name,
          MAX(date(time_on_site_logs.created_at)) AS last_login_date
       FROM time_on_site_logs
       INNER JOIN users
       ON time_on_site_logs.USER_ID = users.ID
       GROUP BY 1,2,3
    )

       SELECT * 
       FROM FINAL
       WHERE last_login_date < CURRENT_DATE-30 ;

   ```

3. [ ] Validate all user accounts require multi-factor authentication.


    <details>

     * [ ] Check “roles and policies” section under settings in Sisense. If 2FA is marked dash (--) for any user then two factor authentication is disabled.



TRUSTED DATA
1. [ ] Review all Golden Record TD tests and make sure they're passing.

    <details>

     ```sql

    SELECT *  
    FROM "PROD"."WORKSPACE_DATA"."DBT_TEST_RESULTS" 
    WHERE test_unique_id LIKE '%raw_golden_data%' 
    AND test_status <>'pass' 
    ORDER BY results_generated_at DESC ;				
				
    ```

2.  [ ] Review Data Siren to confirm known existence of RED data.

    <details>

     ```sql

    SELECT DISTINCT 
       SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME				
    FROM "PREP"."DATASIREN"."DATASIREN_AUDIT_RESULTS"				
    UNION ALL				
    SELECT DISTINCT 
       SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME	
    FROM "PREP"."DATASIREN"."DATASIREN_CANARY_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_COMMON_IP_ADDRESS_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_COMMON_MAPPING_IP_ADDRESS_SENSOR"		
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_LEGACY_EMAIL_VALUE_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME		
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_LEGACY_IP_ADDRESS_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_SOURCE_DB_SOCIAL_SECURITY_NUMBER_SENSOR"		UNION ALL
    SELECT DISTINCT 
       SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME		
    FROM "PREP"."DATASIREN"."DATASIREN_TRANSFORM_DB_EMAIL_VALUE_SENSOR"				
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_TRANSFORM_DB_IP_ADDRESS_SENSOR"
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,				
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_BONEYARD_EMAIL_VALUE_SENSOR"
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_BONEYARD_IP_ADDRESS_SENSOR"
    UNION ALL				
    SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_COMMON_EMAIL_VALUE_SENSOR"
    UNION ALL
     SELECT DISTINCT 
        SENSOR_NAME, 
       (CONCAT(DATABASE_NAME,'.',TABLE_SCHEMA,'.',TABLE_NAME)) AS TABLE_NAME,		
       COLUMN_NAME			
    FROM "PREP"."DATASIREN"."DATASIREN_PROD_COMMON_MAPPING_EMAIL_VALUE_SENSOR"
    ;					
				
     ```


3. [ ] Generate a report of all changes to the TD: Sales Funnel dashboard in the quarter.

    <details>

     * [ ]  Pull the report for business logic changes made to the mart from link (https://gitlab.com/gitlab-data/analytics/-/blame/master/transform/snowflake-dbt/models/marts/sales_funnel/mart_crm_opportunity.sql) by filtering on label “Business logic change”.

          


<!-- DO NOT EDIT BELOW THIS LINE -->
/label ~"Team::Data Platform" ~Snowflake ~TDF ~"Data Team" ~"Priority::1-Ops" ~"workflow::4 - scheduled" 
/confidential 
