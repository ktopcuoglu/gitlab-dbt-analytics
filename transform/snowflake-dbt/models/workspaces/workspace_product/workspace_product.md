{% docs fct_event_usage_metrics %}

Union of gitlab_dotcom and service_ping sources with additional dim id's

{% enddocs %}


{% docs mart_usage_event %}

GitLab.com Event-Level mart - Sourced from fct_event_usage_metrics

This table contains all gitlab.com events with additional dimensions and other facilitating fields.

{% enddocs %}

{% docs mart_usage_event_plan_monthly %}

GitLab.com Plan/Metric mart aggregated by month. Trying to mimic Self-managed usage ping format by
getting the count of each event that happened in the last 28 days. Effectively ignoring the first two
or three days of each month.

Gives the total event counts, as well as the number of unique namespaces and uniques users for that plan/month/metric

{% enddocs %}
