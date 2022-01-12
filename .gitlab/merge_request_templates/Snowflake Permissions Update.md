## Issue
<!---
Link the Issue this MR closes
--->
Closes #

## Checklist

### Format

- [ ] YAML validator passes

### Users

- [ ] If new user, confirm there is a corresponding user role with `securityadmin` as the owner

### Roles

- [ ] If new role, confirm it has been created in Snowflake with `securityadmin` as the owner
- [ ] Confirm user is only granted to user role - can be overridden if necessary


### Database Objects

- [ ] If a referencing a new table make sure it exists in snowflake, or if it is being introduced in this MR that a refresh is run right after merge. 
- [ ] If new schema in `PROD` or `PREP` dbs make sure to update [`grant_usage_in_schemas.sql` macro](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/warehouse/grant_usage_to_schemas.sql)
- [ ] Confirm any new warehouses are created in Snowflake and matches size

### Before merging 

- [ ] Run the [permifrost_manual_spec_test](https://about.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#permifrost_manual_spec_test) pipeline to ensure the specifications have been defined correctly.

### ⚠ Unsupported Permissions ⚠

- [ ] For role naming changes run `show grants` to check for task or stage permissions.
