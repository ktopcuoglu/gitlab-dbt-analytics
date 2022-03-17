{% docs fct_usage_event %}

Union of gitlab_dotcom and service_ping sources with additional dim id's

{% enddocs %}


{% docs mart_usage_event %}

GitLab.com Event-Level mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields.

{% enddocs %}


{% docs mart_xmau_metric_monthly %}

GitLab.com mart that determined unique namespace and user counts for total, free and paid metrics.

{% enddocs %}

{% docs mart_usage_event_plan_monthly %}

GitLab.com Plan/Metric mart aggregated by month. Trying to mimic Self-managed usage ping format by
getting the count of each event that happened in the last 28 days. Effectively ignoring the first two
or three days of each month.

Gives the total event counts, as well as the number of unique namespaces and uniques users for that plan/month/metric

{% enddocs %}

{% docs mart_usage_namespace_daily %}

GitLab.com Namespace-Level mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields and then is aggregated at the namespace level.

{% enddocs %}


{% docs mart_usage_instance_daily %}

GitLab.com Event-Level mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields and then is aggregated at the instance level.

{% enddocs %}


{% docs mart_usage_event_daily %}

GitLab.com Event-Level Daily mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields aggregated at a date/user/namespace/plan/event grain

{% enddocs %}

{% docs fct_service_ping_instance %}

Version source, self managed usage data fact table.

{% enddocs %}

{% docs dim_service_ping_instance %}

Version source, self managed usage data dim table.

{% enddocs %}

{% docs mart_service_ping_instance %}

Version source, self managed usage data mart table.

{% enddocs %}

{% docs prep_service_ping_instance %}

Version source, self managed usage data prep table.

{% enddocs %}
