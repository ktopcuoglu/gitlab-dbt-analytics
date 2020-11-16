## Activities for the Requestor

* [ ] If this is your first time doing this, you will need access to the `snowflake_imports` GCS bucket.  Create an [access request](https://about.gitlab.com/handbook/business-ops/team-member-enablement/onboarding-access-requests/access-requests/#individual-or-bulk-access-request) the usual way.  Tag the `@gitlab-data/engineers` group in the access request when access is ready to be provisioned.
* [ ] Place usage ping payload files in the [`snowflake_imports` GCS bucket](https://console.cloud.google.com/storage/browser/snowflake_imports) by clicking the `UPLOAD FILES` button, selecting the files, and pressing upload.
* [ ] List the file names here:

* [ ] Assign this issue to a member of `@gitlab-data/engineers` for completion.

## For the Assignee
* [ ] Open up a Snowflake DB console
* [ ] Run `list @raw.snowflake_imports.snowflake_imports_stage` and verify you can see the file names listed in the above section
* [ ] For each file to be imported, run the following command, replacing the `{file_name}` placeholder with the actual name of the file:

```
copy into raw.snowflake_imports.usage_ping_payloads (jsontext)
from @raw.snowflake_imports.snowflake_imports_stage/{file_name}
file_format = (type = json)
```

/confidential
/label ~Snowflake ~Infrastructure  ~"Priority::3-Other" ~"Usage/Version Ping"