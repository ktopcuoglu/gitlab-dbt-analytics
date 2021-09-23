<!-- format should be something like 'user deprovisioning - firstname last initial' -->
<!-- example: user deprovisioning - John S -->


Source Access Request: <!-- link to source  Access Request issue or Offboarding issue, it should be approved and ready for deprovisioning -->

# Removing existing User in Snowflake

- [ ] Copy the [user_deprovision.sql](https://gitlab.com/gitlab-data/analytics/-/blob/master/permissions/snowflake/user_deprovision.sql) script, replace the USER_NAME and run it using SECURITYADMIN role.
- [ ] Remove user from [okta-snowflake-users google group](https://groups.google.com/a/gitlab.com/g/okta-snowflake-users/members)
- [ ] Remove user records from `roles.yml`. 


Useful links:
- Snowflake paradigm [handbook page](https://about.gitlab.com/handbook/business-technology/data-team/platform/#snowflake-permissions-paradigm)

/label ~Deprovisioning ~Snowflake ~"Team::Data Platform"  ~"Priority::1-Ops"