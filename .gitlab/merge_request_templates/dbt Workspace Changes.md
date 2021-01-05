<!---
  Use this template when making changes to dbt code in a `workspace` folder.
--->

## Issue
<!--- Link the Issue this MR closes --->
Closes #

## Workspace Code Checks

- [ ] Is the code in a `/transform/snowflake-dbt/models/workspace_<yourspace>` directory?
- [ ] Code runs - recommend you use CI jobs to validate tables are exported where you expect
- [ ] Do you need an update in `dbt_project.yml`?
- [ ] Do you need any CODEOWNERS for this?
- [ ] Do you want any tests added? Not necessary for workspace models but could be a good validation.
- [ ] 

## Submission Checklist
- [ ] 
- [ ] Branch set to delete. (Leave commits unsquashed)
- [ ] Latest CI pipeline passes.
  - [ ] If not, an explanation has been provided.
- [ ] This MR is ready for final check and merge.
- [ ] Remove the `Draft:` prefix in the MR title before assigning to reviewer.
- [ ] Assigned to reviewer. Highlight when you assign that this is for a team workspace model and requires minimal review.

## Reviewer Checklist
- [ ] Check to make sure code runs
- [ ] Validate there are no egregious problems in the code
- [ ] Approve and merge it
