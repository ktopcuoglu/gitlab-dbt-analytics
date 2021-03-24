{% docs prep_usage_ping %}
This data model is a prep model that supports a new dimension model, dim_usage_ping, that will replace PROD.legacy.version_usage_data, dim_usage_pings, version_usage_data_source, and version_raw_usage_data_source in the future . 

This is currently a WIP but inherits a lot of code from PROD.legacy.version_usage_data. See https://gitlab.com/gitlab-data/analytics/-/merge_requests/4064/diffs#bc1d7221ae33626053b22854f3ecbbfff3ffe633 for rationale. This curent version is for Sales team only. 

This is a sensitive model that should not be surfaced into Sisense because it contains IP Address. It also contains a mapping to remove the dependency on the IP Address. 

By the end of the data model, we have set up additional columns that clean up the data and ensures there is one SSOT for any given metric on a usage ping. 
{% enddocs %}


{% docs prep_usage_ping_no_license_key %}
This data model is a prep model that depends on prep_usage_ping and supports the creation of dim_usage_ping that will replace PROD.legacy.version_usage_data, dim_usage_pings, version_usage_data_source, and version_raw_usage_data_source in the future. 

This curent version is for Sales team only. 

Ideally, the purpose of this data model is to unpack all the metrics from the `raw_usage_data_payload` column, strips all the sensitive data out, and has one value for each metric in that column. 
{% enddocs %}


{% docs prep_usage_ping_saas_dates %}
This data model is a prep model that contains the dates of the usage pings for the self-managed instances that powers our SaaS GitLab.com data model. 
{% enddocs %}

{% docs prep_usage_ping_subscription_mapped %}
This data model is a prep model that depends on prep_usage_ping and supports the creation of dim_usage_ping that will replace PROD.legacy.version_usage_data, dim_usage_pings, version_usage_data_source, and version_raw_usage_data_source in the future. 

This curent version is for Sales team only. 

Ideally, the purpose of this data model is to identify the usage pings that can be mapped to a subscription. 
{% enddocs %}

{% docs prep_usage_ping_subscription_mapped_wave_2_3_metrics %}
The purpose of this data model is to identify the usage pings that can be mapped to a subscription and to unpack an initial set ([wave 2-3](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/version/sales_wave_2_3_metrics.sql)) of priority metrics from the `raw_usage_data_payload` column, strip all the sensitive data out, and then report one value for each metric in that column.

This data model is a prep model that depends on prep_usage_ping and supports the creation of dim_usage_ping that will replace `prod.legacy.version_usage_data`, `dim_usage_pings`, `version_usage_data_source`, and `version_raw_usage_data_source` in the future. 

This current version is for Sales team only. 

Ideally, the purpose of this data model is to identify the usage pings that can be mapped to a subscription and to unpack all the metrics from the `raw_usage_data_payload` column, strips all the sensitive data out, and has one value for each metric in that column. 

The metric list identifed can be found in the macro [`sales_wave_2_3_metrics`](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/version/sales_wave_2_3_metrics.sql).
{% enddocs %}

{% docs prep_gainsight_source_model_counts %}
This data model is used to capture the counts for all the source tables used for Gainsight. 

{% enddocs %}

