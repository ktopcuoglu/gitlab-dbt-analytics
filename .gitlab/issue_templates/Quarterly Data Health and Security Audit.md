# Quarterly Data Health and Security Audit

Quarterly audit is performed to validate security like right people with right access in environments (Example: Sisense, Snowflake.etc) and data feeds that are running are healthy (Example: Salesforce, GitLab.com..etc).

Please see the [handbook page](https://about.gitlab.com/handbook/business-technology/data-team/data-management/#quarterly-data-health-and-security-audit) for more information. 

Below checklist of activities would be run once for quarter to validate security and system health.

SNOWFLAKE
1. [ ] Validate terminated employees have been removed from Snowflake access.
    <details>

    Cross check between Employee Directory and Snowflake
    * [ ] If applicable, check if users set to disabled in Snowflake
    * [ ] If applicable, check if users in [roles.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/permissions/snowflake/roles.yml):
        * [ ] isn't assigned to `warehouses`
        * [ ] isn't assigned to `roles`
        * [ ] can_login set to: `no`

    ```sql

      SELECT									 
        employee.employee_id,									 
        employee.first_name,									 
        employee.last_name,									 
        employee.hire_date,									 
        employee.rehire_date,									 
        snowflake.last_success_login,									 
        snowflake.created_on,									 
        employee.termination_date,									
        snowflake.is_disabled									 
      FROM prep.sensitive.employee_directory employee 									 
      INNER JOIN prod.legacy.snowflake_show_users  snowflake									 
      ON employee.first_name = snowflake.first_name									 
      AND employee.last_name = snowflake.last_name									 
      AND snowflake.is_disabled ='false'									 
      AND employee.termination_date IS NOT  NULL;									

    ```

2. [ ] De-activate any account that has not logged-in within the past 60 days from the moment of performing audit from Snowflake.
    <details>

   * [ ] Run below SQL script to perform the check.


    ```sql
     SELECT										
       employee.employee_id,										
       employee.first_name,										
       employee.last_name,										
       employee.hire_date,										
       employee.rehire_date,										
       snowflake.last_success_login,										
       snowflake.created_on,										
       employee.termination_date,										
       snowflake.is_disabled										
     FROM prep.sensitive.employee_directory employee										
     INNER JOIN  prod.legacy.snowflake_show_users snowflake										
     ON employee.first_name = snowflake.first_name										
     AND employee.last_name = snowflake.last_name										
     AND snowflake.is_disabled ='false'										
     AND employee.termination_date IS NULL										
     AND CASE WHEN snowflake.last_success_login IS null THEN snowflake.created_on ELSE snowflake.last_success_login END <= dateadd('day', -60, CURRENT_DATE());										
    ```


3. [ ] Validate all user accounts do not have password set.
    <details>

   * [ ] Check HAS_PASSWRD is set to ‘false’ in users table. If set to ‘false’ then there is not password set. Run below SQL script to perform the check.
   ```sql
     SELECT * 
     FROM "SNOWFLAKE"."ACCOUNT_USAGE"."USERS"
     WHERE has_password = 'true'
     AND disabled = 'false'
     AND deleted_on IS NULL
     AND name NOT IN ('PERMISSION_BOT','FIVETRAN','GITLAB_CI','AIRFLOW','STITCH','SISENSE_RESTRICTED_SAFE','PERISCOPE','MELTANO',   'TARGET_SNOWFLAKE','GRAFANA','SECURITYBOTSNOWFLAKEAPI', 'GAINSIGHT');
 
    ```

SISENSE
1. [ ] Validate off-boarded employees have been removed from Sisense access.
    <details>

   * [ ] Step 1: In order to get latest data loaded into table `legacy.sheetload_sisense_users`, Google Sheet needs to be updated with latest data from Sisense `users` table. To update the latest data, run below SQL in Sisense under database `periscope_usage_data` and paste the data in google sheet (https://docs.google.com/spreadsheets/d/1oY6YhTuXYqy5ujlTxrQKf7KCDNpPwKWD_hZmzR1UPIo/edit#gid=0). Make sure Step 1 is completed atlease 1 day before running SQL in Step 2, as sheetload runs once in 24 hours to get latest data loaded from google sheetload into `legacy.sheetload_sisense_users` table.


    ```sql

      SELECT * from users;
   
    ```

   * [ ] Step 2: Run below SQL script to perform the check.
   

   ```sql

     WITH final AS (
        
        SELECT 
          MAX(date_actual) AS date_actual, 
          full_name,
          work_email, 
          is_termination_date 
        FROM legacy.employee_directory_analysis 
        GROUP BY 2,3,4

    )

    SELECT   
       final.full_name, 
       users.email_address , 
       final.is_termination_date ,
       final.date_actual
    FROM  final
    INNER JOIN legacy.sheetload_sisense_users users 
      ON  final.full_name = concat(users.first_name,' ', users.last_name) 
      AND final.is_termination_date = 'TRUE' 
    LEFT JOIN legacy.sheetload_sisense_user_roles roles
      ON users.id = roles.user_id
    WHERE roles.user_id IS NOT NULL
    GROUP BY 1,2,3,4
    ORDER BY 1 ;

   ```


2. [ ] De-activate any account that has not logged-in within the past 90 days from the moment of performing audit from Sisense.

    <details>

   * [ ] Run below SQL script to perform the check.

   ```sql
      WITH final as (
         SELECT users.id, 
            first_name, 
            last_name, 
            email_address, 
            MAX(DATE(time_on_site_logs.created_at)) AS last_login_date  
         FROM time_on_site_logs
         JOIN users
         --inner join between time_on_site_logs and users. This means if a user never performed a login, it will not show up in the results
         --improvement point for next iteration check for users that were created over 90 days ago and that didn't perform a login.
         ON time_on_site_logs.USER_ID = users.ID
         LEFT OUTER JOIN user_roles
         ON users.id = user_roles.user_id
         LEFT OUTER JOIN roles
         ON user_roles.role_id = roles.id
         --check if a user has a role assigned (because the users table contains all users ever exist in Sisense).
         WHERE roles.name = 'Everyone'
         GROUP BY 1,2,3,4
      )

      SELECT * 
      FROM final
      WHERE last_login_date < CURRENT_DATE-90
      ORDER BY last_name;
   ```


TRUSTED DATA
1. [ ] Review all Golden Record TD tests and make sure they're passing.

    <details>

    * [ ] Run below SQL script to perform the check.

     ```sql

    SELECT *  
    FROM "PROD"."WORKSPACE_DATA"."DBT_TEST_RESULTS" 
    WHERE test_unique_id LIKE '%raw_golden_data%' 
    AND test_status <>'pass' 
    ORDER BY results_generated_at DESC ;				
				
    ```

2.  [ ] Review Data Siren to confirm known existence of RED data.

    <details>
    
    * [ ] Run below SQL script to perform the check.

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
/label ~"Team::Data Platform" ~Snowflake ~TDF ~"Data Team" ~"Priority::1-Ops" ~"workflow::4 - scheduled" ~"Quarterly Data Health and Security Audit" ~"Periscope / Sisense"
/confidential 
