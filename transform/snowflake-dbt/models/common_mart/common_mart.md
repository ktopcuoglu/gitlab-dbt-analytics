{% docs mart_usage_event %}
Type of Data: gitlab.com db usage events

Aggregate Grain: Event (this would be the mart version of the atomic table)

Time Grain: None

Use case: Everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges, exclude specific projects

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

{% enddocs %}
