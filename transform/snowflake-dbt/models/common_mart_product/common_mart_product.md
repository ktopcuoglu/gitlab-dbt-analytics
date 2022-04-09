{% docs mart_xmau_metric_monthly %}
Type of Data: gitlab.com db usage events

Aggregate Grain: reporting_month, user_group, section_name, stage_name, and group_name

Time Grain: Last 28 days of the month

Use case: Paid SaaS xMAU, SaaS SpO

Usage is attributed to a namespace's last reported plan (free vs paid)
(GitLab.com mart that determines unique namespace and user counts for total, free and paid metrics.)
{% enddocs %}