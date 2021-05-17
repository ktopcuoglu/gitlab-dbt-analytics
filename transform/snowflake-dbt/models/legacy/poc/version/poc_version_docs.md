{% docs monthly_usage_data_28_days %}

This model transforms 28_days counters stored in the usage data payload into monthly proxies. To do so the logic is as follows:

* For one specific instance (instance_uid), one specific metric (metrics_name) and one specific month M, take the latest received data

{% enddocs %}

{% docs monthly_usage_data_all_time %}

This model transforms all_time counters stored in the usage data payload into monthly proxies. To do so the logic is as follows:

* For one specific instance (instance_uid), one specific metric (metrics_name) and one specific month M, take the latest received data
* Compare this data point with the one received one month before, we have 3 scenarios:
  * data from month M > data from month M-1: the monthly proxied metric is data(M) - data(M-1)
  * no data for month M-1: then monthly proxied metric is data(M)
  * data from month M < data from month M-1: abnormal behavior which might be due to an instance reset, then proxied metric is 0

{% enddocs %}

{% docs monthly_usage_data %}

Union of models `monthly_usage_data_28_days` and `monthly_usage_data_all_time`

{% enddocs %}

{% docs prep_usage_data_7_days_flattened %}

The granularity of this model is one row per tuple (metric_name, instance_id).

Usage ping's data is stored in several nested jsons as shown in [this page](https://docs.gitlab.com/ee/development/telemetry/usage_ping.html#example-usage-ping-payload). 

Those metrics sent could be of various types:
* all_time counters (for example how many issues a specific instance has created since its inception)
* 28_days counters (how many users have created at least one issue over the last 4 weeks)
* 7_days counters (how many users have created at least one issue over the last 7 days)
* an instance configuration parameter (has this instance enabled saml/sso)

This model extracts the 7-days counters (based on the mapping table in this spreadsheet) and flattens the json. 

The models transforms this json:

```
{
  "uuid": "0000000-0000-0000-0000-000000000000",
  "hostname": "example.com",
  "version": "12.10.0-pre",
  ...
  "analytics_unique_visits": {
    "g_analytics_contribution": 999,
    ...
  },
  "usage_activity_by_stage_monthly": {
    "configure": {
      "project_clusters_enabled": 999,
      ...
    },
    "create": {
      "merge_requests": 999,
      ...
    },
    "manage": {
      "events": 999,
      ...
    }
  }
  }
```

into this table:

| uuid                                | hostname    | version     | full_metric_path                                                   | clean_metric_name                                      | metric_value |
|-------------------------------------|-------------|-------------|--------------------------------------------------------------------|--------------------------------------------------------|--------------|
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | analytics_unique_visits.g_analytics_contribution                   | g_analytics_contribution_unique_visitors_count_28_days | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.configure.project_clusters_enabled | project_clusters_ebled_users_count_28_days                     | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.create.merge_requests              | merge_requests_creation_users_count_28_days            | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.manage.events                      | events_users_count_28_days                             | 999          |


{% enddocs %}

{% docs prep_usage_data_28_days_flattened %}

The granularity of this model is one row per tuple (metric_name, instance_id).

Usage ping's data is stored in several nested jsons as shown in [this page](https://docs.gitlab.com/ee/development/telemetry/usage_ping.html#example-usage-ping-payload). 

Those metrics sent could be of various types:
* all_time counters (for example how many issues a specific instance has created since its inception)
* 7_days counters (how many users have created at least one issue over the last 4 weeks)
* an instance configuration parameter (has this instance enabled saml/sso)

This model extracts the 28-days counters (based on the mapping table in this spreadsheet) and flattens the json. 

The models transforms this json:

```
{
  "uuid": "0000000-0000-0000-0000-000000000000",
  "hostname": "example.com",
  "version": "12.10.0-pre",
  ...
  "analytics_unique_visits": {
    "g_analytics_contribution": 999,
    ...
  },
  "usage_activity_by_stage_monthly": {
    "configure": {
      "project_clusters_enabled": 999,
      ...
    },
    "create": {
      "merge_requests": 999,
      ...
    },
    "manage": {
      "events": 999,
      ...
    }
  }
  }
```

into this table:

| uuid                                | hostname    | version     | full_metric_path                                                   | clean_metric_name                                      | metric_value |
|-------------------------------------|-------------|-------------|--------------------------------------------------------------------|--------------------------------------------------------|--------------|
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | analytics_unique_visits.g_analytics_contribution                   | g_analytics_contribution_unique_visitors_count_28_days | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.configure.project_clusters_enabled | project_clusters_ebled_users_count_28_days                     | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.create.merge_requests              | merge_requests_creation_users_count_28_days            | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.manage.events                      | events_users_count_28_days                             | 999          |


{% enddocs %}

{% docs prep_usage_data_all_time_flattened %}

The granularity of this model is one row per tuple (metric_name, instance_id).

Usage ping's data is stored in several nested jsons as shown in [this page](https://docs.gitlab.com/ee/development/telemetry/usage_ping.html#example-usage-ping-payload). 

Those metrics sent could be of various types:
* all_time counters (for example how many issues a specific instance has created since its inception)
* 28_days counters (how many users have created at least one issue over the last 4 months)
* an instance configuration parameter (has this instance enabled saml/sso)

This model extracts the all-time counters (based on the mapping table in this spreadsheet) and flattens the json. 

The models transforms this json:

```
{
  "uuid": "0000000-0000-0000-0000-000000000000",
  "hostname": "example.com",
  "version": "12.10.0-pre",
  ...
  "analytics_unique_visits": {
    "g_analytics_contribution": 999,
    ...
  },
  "usage_activity_by_stage_monthly": {
    "configure": {
      "project_clusters_enabled": 999,
      ...
    },
    "create": {
      "merge_requests": 999,
      ...
    },
    "manage": {
      "events": 999,
      ...
    }
  }
  }
```

into this table:

| uuid                                | hostname    | version     | full_metric_path                                                   | clean_metric_name                                      | metric_value |
|-------------------------------------|-------------|-------------|--------------------------------------------------------------------|--------------------------------------------------------|--------------|
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | analytics_unique_visits.g_analytics_contribution                   | g_analytics_contribution_unique_visitors_count_all_time | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.configure.project_clusters_enabled_all_time | project_clusters_enabled_users_count_all_time                     | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.create.merge_requests              | merge_requests_creation_users_count_all_time            | 999          |
| 0000000-0000-0000-0000-000000000000 | example.com | 12.10.0-pre | usage_activity_by_stage_monthly.manage.events                      | events_users_count_all_time                            | 999          |


{% enddocs %}
