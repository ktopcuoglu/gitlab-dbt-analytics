{% docs alter_warehouse %}
This macro turns on or off a Snowflake warehouse.
{% enddocs %}


{% docs backup_to_gcs %}
This macro fetches all relevant tables in the specified database and schema for backing up into GCS. This macro should NOT be used outside of a `dbt run-operation` command.
{% enddocs %}


{% docs get_backup_table_command %}
This macro is called by `backup_to_gcs` so that the actual `copy into` command can be generated. This macro should NOT be referenced outside of the `backup_to_gcs` macro.
{% enddocs %}


{% docs grant_usage_to_schemas %}
This macro...
{% enddocs %}


{% docs gdpr_delete %}
This macro is intended to be run as a dbt run-operation. The command to do this at the command line is:

dbt run-operation gdpr_delete --args '{email_sha: your_sha_here}'

The sha can be generated separately on the command line as well by doing the following:

echo -n email@redacted.com | shasum -a 256

The macro gathers all of the columns within the RAW database that match `email` anywhere in the name and delete the rows if they're not in the `snapshots` schema. If the columns are in the snapshot schema it will then 
{% enddocs %}
