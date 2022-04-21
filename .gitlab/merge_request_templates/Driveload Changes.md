<!---
  Use this template when adding a sheetload file or making changes to a sheetload file
--->

## Issue
<!--- Link the Issue this MR closes --->
Closes #

## Solution

Describe the solution. Include links to any related MRs and/or issues.

* [ ] Provide link to CSV/GSheet data. Link: ____
* [ ] Does this data contain anything that is sensitive (Classified as Red or Orange in our [Data Classification Policy](https://about.gitlab.com/handbook/engineering/security/data-classification-standard.html#data-classification-levels))?
  - [ ] Yes 
  - [ ] No
  - [ ] I don't know
* [ ]  How long will this data need to reside in the Data team's data warehouse? Expiration Date: ______ 

## Adding/Updating a new sheetload file*
 

#### Testing

<details>
<summary><i>Click to toggle Testing</i></summary>

* [ ] Every model should be [tested](https://docs.getdbt.com/docs/testing-and-documentation) AND documented in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable.
* [ ] All models should be integrated into the [trusted data framework](https://about.gitlab.com/handbook/business-technology/data-team/platform/#tdf)
  * [ ] If there is an associated MR in the [Data Tests](https://gitlab.com/gitlab-data/data-tests) project, be sure to pass the branch name to the manual job using the `DATA_TEST_BRANCH` environment variable.
* [ ] If the periscope_query job failed, validate that the changes you've made don't affect the grain of the table or the expected output in Periscope.
* [ ] If you are on the Data Team, please paste the output of `dbt test` when run locally below. Any failing tests should be fixed or explained prior to requesting a review.
</details>


## CI Jobs to run*
Run the following CI Jobs on the MR: 

* [ ] `❄️ Snowflake: clone_raw_specific_schema` 
  * Pass the variable SCHEMA_NAME with the value `driveload`
* [ ] `Extract: driveload`
* [ ] `⚙️ dbt Run: specify_raw_model`
    * Pass `DBT_MODELS` as key and `sources.driveload` for value (alternatively, you can pass `sources.driveload_file_name_source`)
* [ ] In the case there is staging model, run `⚙️ dbt Run: specify_model`
    * Pass `DBT_MODELS` as key and `staging.driveload` for value (alternatively, you can pass `staging.driveload_file_name`)

## Final Steps
* [ ]  Assign MR to project maintainer for review (iterate until model is complete).
* [ ]  Data Team project maintainers/owners to merge in dbt models 
* [ ]  If not urgent, data will be availble within 24 hours. If urgent, Data Engineer to run full refresh and inform when available.
* [ ]  Submitter to query in Sisense for table: ``` SELECT * FROM [new-dbt-model-name] LIMIT 10 ```.


## All MRs Checklist
* [ ] This MR follows the coding conventions laid out in the [SQL style guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/sql-style-guide/), including the [dbt guidelines](https://about.gitlab.com/handbook/business-ops/data-team/platform/sql-style-guide/#dbt-guidelines).
* [ ] [Label hygiene](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-labeling) on issue.
* [ ] Branch set to delete. (Leave commits unsquashed)
* [ ] Latest CI pipeline passes.
  * [ ] If not, an explanation has been provided.
* [ ] This MR is ready for final review and merge.
* [ ] All threads are resolved.
* [ ] Remove the `Draft:` prefix in the MR title before assigning to reviewer.
* [ ] Assigned to reviewer.

## Reviewer Checklist
- [ ]  Check before setting to merge

## Further changes requested
* [ ]  AUTHOR: Uncheck all boxes before taking further action.


