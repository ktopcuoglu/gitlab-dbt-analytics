## Activities for the Requestor

* [ ] Follow the instructions in the [handbook page](https://about.gitlab.com/handbook/business-ops/data-team/data-catalog/manual-data-upload/)
* [ ] List the file names here:

* Indicate a desired response time:
  * [ ] 24h
  * [ ] 48h
  * [ ] No particular SLA
  * [ ] Other, specify ________
* [ ] Tag the `@gitlab-data/engineers` group in this issue.

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
/label ~Snowflake ~Infrastructure  ~"Priority::3-Other" ~"Usage/Version Ping" ~"Customer Success" 