# Quarterly Data Health and Security Audit

Quarterly audit is performed to validate security like right people with right access in environments (Example: Sisense, Snowflake.etc) and data feeds that are running are healthy (Example: Salesforce, GitLab.com..etc).

Below checklist of activities would be run once for quarter to validate security and system health.

SNOWFLAKE
1. [ ] Validate terminated employees have been removed from Snowflake access.
2. [ ] De-activate any account that has not logged-in within the past 30 days from the moment of performing audit from Snowflake.
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
/label ~Infrastructure ~Snowflake ~TDF ~"Data Team" ~"Priority::1-Ops" ~"workflow::4 - scheduled" 
/confidential 
