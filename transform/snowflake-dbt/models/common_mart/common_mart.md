{% docs mart_event %}
Enriched version of the atomic (event-level) GitLab.com usage events table, `common.fct_event`

Type of Data: gitlab.com db usage events

Aggregate Grain: None

Time Grain: None

Use case: Everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges, exclude specific projects, etc

Note: This model excludes events occurring before a gitlab.com user was created (ex: imported projects; see fct_event for more details). Events not tied to a specific user are included.

{% enddocs %}

{% docs mart_event_namespace_daily %}
Enhanced version of `common.fct_event_namespace_daily`

Type of Data: gitlab.com db usage events

Aggregate Grain: event_name, dim_ultimate_parent_namespace_id

Time Grain: event_date

Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

Note: This model excludes events occurring before a gitlab.com user was created (ex: imported projects; see fct_event for more details). Events not tied to a specific user are included.

{% enddocs %}

{% docs mart_event_daily %}
Enhanced version of `common.fct_event_daily`

Type of Data: gitlab.com db usage events

Aggregate Grain: event_name, dim_ultimate_parent_namespace_id, dim_user_id

Time Grain: event_date

Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

Note: This model excludes events occurring before a gitlab.com user was created (ex: imported projects; see fct_event for more details). Events not tied to a specific user are excluded.

{% enddocs %}
