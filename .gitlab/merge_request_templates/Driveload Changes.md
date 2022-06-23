<!---
  Use this template when adding a driveload file or making changes to a driveload file
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


## Adding/Updating a new Driveload file
<details><summary>Click on Dropdown for Process</summary>
<br>

* [ ] Step 1: Create a new folder in Google Drive and upload your file to it. Files can only be .csv format currently.  
* [ ] Step 2: Share the file with the required service accounts [Email Address to share with](https://docs.google.com/document/d/1m8kky3DPv2yvH63W4NDYFURrhUwRiMKHI-himxn1r7k/edit?usp=sharing) (GitLab Internal). 
  * Ensure you share the folder with both the runner & airflow service accounts. The runner account is required for running the Driveload CI pipeline, the airflow account is used while running in production.
* [ ] Step 3: Open up the web ide and let's start the MR! Update extract--> sheetload--> [drives.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/sheetload/drives.yml)
    * Add the name of the file_name (i.e. kpi_status)
    * Add the name of the newly created folder, folder names need to unique here so ensure you do not conflict with any existing folders. 
    * Add yourself as an owner
    * Decide on a loading strategy for table_replace_append. 
      * If set to 1, all files which are uploaded to the folder will be **appended** to the table. After the files are uploaded they will go into the `./Archive` directory inside the GDrive folder.
      * If set to 0, the files which are uploaded to the folder will **replace** the data in the table. 
* [ ] Step 4: Next in this MR, head to transform --> snowflake-dbt --> models --> sources --> driveload--> [Edit the sources.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/sources/driveload/sources.yml). Add the file name as `driveload_file_name_source`
* [ ] Step 5: In the same repoistory folder as the sources.yml file, you will [add the base model to sources.driveload repository](https://gitlab.com/gitlab-data/analytics/-/tree/master/transform/snowflake-dbt/models/sources/driveload). Naming the file as driveload_file_name_sources.sql.
        This file will have the following code, but can also be restricted down to specific columns. Update data type of columns in this file (i.e converting value to decimal or varchar)
```sql
        WITH source AS (

        SELECT * 
        FROM {{ source('driveload','file_name') }}

        )
        SELECT * 
        FROM source
```
* [ ] Step 6: In the sources.driveload repository [Edit the schema.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/sources/driveload/schema.yml) to explain the source model. 
* [ ] Step 6.5: (If you do not have an applicable workspace model folder, create a workspace folder under models/workspaces)
* [ ] Step 7: Next we'll head to head to transform --> snowflake-dbt --> models --> workspaces --> <YourWorkspace>--> Schema.yaml. Add your model name here, along with a .sql file to select the model into this layer. 
  * This will make the model accesible in Sisense. If any transformations are needed, this would be the file to update. Name the file `driveload_file_name`
* [ ] Step 8: Update the schema.yaml to add description of the workspace model. 

To understand the difference between source and staging models, please refer to these sources: [source models](https://about.gitlab.com/handbook/business-ops/data-team/platform/dbt-guide/#source-models) vs [staging models](https://about.gitlab.com/handbook/business-ops/data-team/platform/dbt-guide/#staging)
</details>

#### Testing

<details>
<summary><i>Click to toggle Testing</i></summary>

* [ ] Every model should be [tested](https://docs.getdbt.com/docs/testing-and-documentation) AND documented in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable.
* [ ] All models should be integrated into the [trusted data framework](https://about.gitlab.com/handbook/business-technology/data-team/platform/#tdf)
  * [ ] If there is an associated MR in the [Data Tests](https://gitlab.com/gitlab-data/data-tests) project, be sure to pass the branch name to the manual job using the `DATA_TEST_BRANCH` environment variable.
* [ ] If the periscope_query job failed, validate that the changes you've made don't affect the grain of the table or the expected output in Periscope.
* [ ] If you are on the Data Team, please paste the output of `dbt test` when run locally below. Any failing tests should be fixed or explained prior to requesting a review.
</details>


## CI Jobs to run
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


