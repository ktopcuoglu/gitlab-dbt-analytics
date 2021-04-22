## Request for data export
<!--
Please complete all items. Ask questions in the #data slack channel
--->
### Receiving contact Info: 
* [ ] Assign team member(s) who should be receiving this export to the issue and tag them below:
  - `@gitlab handle`
* [ ] Slack handle(s) for those assigned: 
  - `@handle`

### Business Use Case (Please explain what this data will be used for and appropriate data filters): 

<!--
please describe what you will be doing with this data (where is it going, how will it be used, etc)
--->

---

### Submitter Checklist
* [ ] Data Location - Which data do you need exported? Please provide at least one of the following:
  - [ ] URL to dashboard that you wanted to export from Periscope that was greater than 5MB. Link: _____ 
  - [ ] Database, Schema, & Table (or view) in Snowflake (dot notation preferred): `database.schema.table`
  - [ ] [Link to SQL Code Snippet](https://gitlab.com/gitlab-data/analytics/-/snippets/2108714)
* [ ]  Provide a file name for the file. File Name: ___________ 
* [ ]  Select the type of format you would like the data exported in: 
  - [ ] TSV (tab-separated values)
  - [ ] CSV (comma-separated values)
* [ ]  Does this data contain `RED` or `ORANGE` data as defined in [GitLab's data classification policy?](https://about.gitlab.com/handbook/engineering/security/data-classification-policy.html#data-classification-levels)
  - [ ] Yes (You will need to already have [approved access](https://about.gitlab.com/handbook/business-ops/it-ops-team/access-requests/) to this data) 
  - [ ] No (I verify that this data that will be extracted does not contain sensitive information.)
* [ ]  Add a due date to the issue for when the data is desired by.
* [ ]  cc: `@gitlab-data` to have this issue prioritized and assigned.  

---

### Reviewer Checklist 
* [ ] Review the Submitter Checklist. 
* [ ] Verify Requestors access 

#### Small Data Set
* [ ] Export the file in the requested format using the Snowflake UI's export functionality.

#### Large Data Set
* [ ] Load the dataset in the requested format to the `"RAW"."PUBLIC".SNOWFLAKE_EXPORTS` stage by running:

```
COPY INTO @"RAW"."PUBLIC".SNOWFLAKE_EXPORTS/<identifiable_folder_name>/
FROM (<query>)
FILE_FORMAT = (TYPE = {CSV | JSON | PARQUET} <other format options>) 
```

Replace the folder name with something unique and recognizable, such as the name of the issue.  Make sure to [add format options](https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html#syntax) to get the data to be formatted as requested.  This is especially important with the CSV format for escaping records and defining delimiters.

By default, the command will create multiple files in GCS.  To just create one file, add the `single = true` option at the end of the command.

After the dataset has been copied into the GCS stage, it can be downloaded from gcs in the [`snowflake_exports` bucket in the `gitlab-analysis` project](https://console.cloud.google.com/storage/browser/snowflake_exports).  Download the files and follow the instructions below. 


#### Sharing
* For Sensitive Data
  - [ ] Encrypt the data into zip file (use `zip -er`)
  - [ ] Share the file's password with the submitter over a secure channel separate from the channel you will use to send the file.  [One time secret](https://onetimesecret.com/) may be a good option to share passwords, just make sure to not put in any context with the password. 
* [ ] Share the file with the submitter over a secure channel
* [ ] Reassign this issue to the Submitter for verification of receipt 
* [ ] Once the submitter has verified receipt, delete the file from your computer

<!-- DO NOT EDIT BELOW THIS LINE -->

/label ~Infrastructure ~Snowflake ~Analytics ~"Data Pump" ~"workflow::1 - triage" 
/confidential 
