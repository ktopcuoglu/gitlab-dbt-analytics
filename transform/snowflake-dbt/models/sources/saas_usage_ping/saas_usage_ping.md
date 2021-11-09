{% docs saas_usage_ping_instance %}
Source table for the RAW.SAAS_USAGE_PING.INSTANCE_SQL_METRICS table representing instance level usage ping.
Data is stored in .json format.
{% enddocs %}

{% docs instance_sql_errors %}
Source table for handling errors during the generating SQL metrics for the service ping. If any record appear, will be considered as an error in the process.
{% enddocs %}

{% docs instance_redis_metrics %}
Source table for the RAW.SAAS_USAGE_PING.INSTANCE_REDIS_METRICS table representing instance level usage ping.
Data is stored in .json format.
{% enddocs %}

{% docs saas_usage_ping_namespace %}
Source table for the RAW.SAAS_USAGE_PING.GITLAB_DOTCOM_NAMESPACE table representing namespace level usage ping.
Data is flattened and represent as a regular table.
{% enddocs %}