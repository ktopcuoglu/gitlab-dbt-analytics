{% docs rpt_event_xmau_metric_monthly %}
Reporting model that calculates unique user and namespace counts for GitLab.com xMAU metrics.

Type of Data: gitlab.com db usage events

Aggregate Grain: user_group (total, free, paid), section_name, stage_name, and group_name

Time Grain: reporting_month (defined as the last 28 days of the calendar month). This is intended to match the instance-level service ping metrics by getting a 28-day count of each event.

Use case: Paid SaaS xMAU, SaaS SpO

Note: Usage is attributed to a namespace's last reported plan (free vs paid)
{% enddocs %}

{% docs rpt_event_plan_monthly %}
Type of Data: gitlab.com db usage events

Aggregate Grain: plan_id_at_event_date, event_name

Time Grain: reporting_month (defined as the last 28 days of the calendar month). This is intended to match the instance-level service ping metrics by getting a 28-day count of each event.

Use case: Paid SaaS xMAU, SaaS SpO

{% enddocs %}
