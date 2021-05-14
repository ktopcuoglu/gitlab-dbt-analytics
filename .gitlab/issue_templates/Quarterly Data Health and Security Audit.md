# Quarterly Data Health and Security Audit

Quarterly audit is performed to validate security like right people with right access in environments (Example: Sisense, Snowflake.etc) and data feeds that are running are healthy (Example: Salesforce, GitLab.com..etc).

Please see the [handbook page](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/duties/#quarterly-data-health-and-security-audit) for more information. 

Below checklist of activities would be run once for quarter to validate security and system health.

SNOWFLAKE
1. [ ] Validate terminated employees have been removed from Snowflake access.
    <details>

    Cross check between BambooHR and Snowflake
    * [ ] If applicable, check if users set to disabled in Snowflake
    * [ ] If applicable, check if users in [roles.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/load/snowflake/roles.yml):
        * [ ] isn't assigned to `warehouses`
        * [ ] isn't assigned to `roles`
        * [ ] can_login set to: `no`

    </details>

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
      and CASE WHEN last_success_login IS null THEN created_on ELSE last_success_login END <= dateadd('day', -30, CURRENT_DATE())
    
    ```
    
    </details>

3. [ ] Validate all user accounts require multi-factor authentication.

SISENSE
1. [ ] Validate terminated employees have been removed from Sisense access.
2. [ ] De-activate any account that has not logged-in within the past 30 days from the moment of performing audit from Sisense.
3. [ ] Validate all user accounts require multi-factor authentication.

TRUSTED DATA
1. [ ] Review all Golden Record TD tests and make sure they're passing.
2. [ ] Review Data Siren to confirm known existence of RED data.
3. [ ] Generate a report of all changes to the TD: Sales Funnel dashboard in the quarter.

<!-- DO NOT EDIT BELOW THIS LINE -->
/label ~"Team::Data Platform" ~Snowflake ~TDF ~"Data Team" ~"Priority::1-Ops" ~"workflow::4 - scheduled" 
/confidential 
