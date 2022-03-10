{% docs fct_usage_event %}

Type of Data: Union of gitlab_dotcom and service_ping sources with additional dim id's
Use case: Source of truth (atomic)

{% enddocs %}


{% docs mart_usage_event %}

Type of Data: gitlab.com db usage events
Aggregate Grain: Event (this would be the mart version of the atomic table)
Time Grain: None
Use case: Everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges, exclude specific projects
(GitLab.com Event-Level mart - Sourced from fct_event_usage_metrics)

{% enddocs %}


{% docs mart_xmau_metric_monthly %}

Type of Data: gitlab.com db usage events
Aggregate Grain: Plan (including Free/Paid and Total) / Metric
Time Grain: 28-day (likely last 28 days of the month)
Use case: Paid SaaS xMAU, SaaS SpO
(GitLab.com mart that determined unique namespace and user counts for total, free and paid metrics.)

{% enddocs %}

{% docs mart_usage_event_plan_monthly %}

Type of Data: gitlab.com db usage events
Aggregate Grain: Plan (including Free/Paid and Total) / Metric
Time Grain: 28-day (likely last 28 days of the month)
Use case: Paid SaaS xMAU, SaaS SpO
(GitLab.com Plan/Metric mart aggregated by month. Trying to mimic Self-managed usage ping format by
getting the count of each event that happened in the last 28 days. Effectively ignoring the first two
or three days of each month.)

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

Type of Data: gitlab.com db usage events
Aggregate Grain: User / Namespace / Plan / Event
Time Grain: Day
Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges
(GitLab.com Event-Level Daily mart - Sourced from fct_event_usage_metrics)

{% enddocs %}
