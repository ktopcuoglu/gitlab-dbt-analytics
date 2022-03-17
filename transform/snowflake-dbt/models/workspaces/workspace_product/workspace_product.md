{% docs fct_usage_event %}

Type of Data: gitlab.com db usage events
Aggregate Grain: Event
Use case: Source of truth (atomic), contains foreign keys to easily join to DIM tables or other FCT/MART tables for additional detail and discovery

{% enddocs %}


{% docs mart_usage_event %}

Type of Data: gitlab.com db usage events
Aggregate Grain: Event (this would be the mart version of the atomic table)
Time Grain: None
Use case: Everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges, exclude specific projects

{% enddocs %}


{% docs mart_xmau_metric_monthly %}

Type of Data: gitlab.com db usage events
Aggregate Grain: reporting_month, user_group, section_name, stage_name, and group_name
Time Grain: Last 28 days of the month
Use case: Paid SaaS xMAU, SaaS SpO
Usage is attributed to a namespace's last reported plan (free vs paid)
(GitLab.com mart that determines unique namespace and user counts for total, free and paid metrics.)

{% enddocs %}

{% docs mart_usage_event_plan_monthly %}

Type of Data: gitlab.com db usage events
Aggregate Grain: reporting_month, plan_id_at_event_date, and event_name
Time Grain: Last 28 days of the month
Use case: Paid SaaS xMAU, SaaS SpO
(Trying to mimic Self-managed usage ping format by getting the count of each event that happened in the last 28 days.
 Effectively ignoring the first two or three days of each month.)

{% enddocs %}

{% docs mart_usage_namespace_daily %}

Type of Data: gitlab.com db usage events
Aggregate Grain: event_date, event_name, and dim_namespace_id
Time Grain: Day
Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

{% enddocs %}


{% docs mart_usage_instance_daily %}

Type of Data: gitlab.com db usage events
Aggregate Grain: event_date, event_name, dim_instance_id (all od SaaS)
Time Grain: Day
Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

{% enddocs %}


{% docs mart_usage_event_daily %}

Type of Data: gitlab.com db usage events
Aggregate Grain: event_date, dim_user_id, dim_namespace_id, and event_name
Time Grain: Day
Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges
