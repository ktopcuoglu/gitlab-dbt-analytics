<!-- format should be something like 'user provisioning - firstname last initial' -->
<!-- example: user rovisioning - John S -->


Source Access Request: <!-- link to source  Access Request issue, it should be approved and ready for provisioning -->

# Creating a New User in Snowflake

- [ ] Create user using SECURITYADMIN role
- [ ] Create user specific role
- [ ] Assign user specific role
- [ ] Verify grants
- [ ] Add to [okta-snowflake-users google group](https://groups.google.com/a/gitlab.com/g/okta-snowflake-users/members)
- [ ] Update `roles.yml` and add a comment with a access request URL

Useful links:
- Create user [SQL template](https://gitlab.com/gitlab-data/analytics/-/blob/master/permissions/snowflake/user_provision.sql)
- Snowflake paradigm [handbook page](https://about.gitlab.com/handbook/business-technology/data-team/platform/#snowflake-permissions-paradigm)

/label ~Provisioning ~Snowflake ~"Team::Data Platform"  ~"Priority::1-Ops"

