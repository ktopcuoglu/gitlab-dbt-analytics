<!---
  Use this template when migrating dbt models from the analytics database to the prep or prod databases.
--->

## Issue
<!--- Link the Issue this MR closes --->
Closes #

## Solution

Describe the solution. Include links to any related MRs and/or issues.

## Submitter Checklist

For the models being migrated:
- [ ] Configuration changes are at the highest level possible
  - Most changes should be in dbt_project.yml
- [ ] Model level configuration reviewed
- [ ] All Database references are confirmed to be either PREP or PROD
  - Use the environment variable reference to refer to these databases
- [ ] Follow-up issue created with tables to remove from ANALYTICS
- [ ] Periscope MR staged with relevant changes

**Which pipeline job do I run?** See our [handbook page](https://about.gitlab.com/handbook/business-ops/data-team/platform/ci-jobs/) on our CI jobs to better understand which job to run.

## All MRs Checklist
- [ ] This MR follows the coding conventions laid out in the [SQL style guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/sql-style-guide/), including the [dbt guidelines](https://about.gitlab.com/handbook/business-ops/data-team/platform/sql-style-guide/#dbt-guidelines).
- [ ] [Label hygiene](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-labeling) on issue.
- [ ] Branch set to delete. (Leave commits unsquashed)
- [ ] Latest CI pipeline passes.
  - [ ] If not, an explanation has been provided.
- [ ] This MR is ready for final review and merge.
- [ ] All threads are resolved.
- [ ] Remove the `Draft:` prefix in the MR title before assigning to reviewer.
- [ ] Assigned to reviewer.


