{% docs gitlab_dotcom_xmau_metrics_doc %}
This seed file captures the different metrics used for calculation of XMAU metrics. More information around XMAU metrics [here](https://about.gitlab.com/handbook/product/performance-indicators/#three-versions-of-xmau)
{% enddocs %}

{% docs version_blacklisted_instance_uuid %}
This seed file captures the UUID sending us abnormal usage ping counters. Those UUID are excluded from the analytics downstream models in order to allow easy charting.
For example in [this issue](https://gitlab.com/gitlab-data/analytics/-/issues/4343), one can clearly see an abnormal spike in monthly numbers of `projects_prometheus_active`.
{% enddocs %}

{% docs version_usage_stats_to_stage_mappings_doc %}
The version_usage_stats_to_stage_mapping_data.csv maps usage ping fields to different value and team stages. See https://about.gitlab.com/handbook/product/categories/#hierarchy for more information about stages. If the stage is `ignored` it was determined that they are not part of any stage or there is no relevant data. See https://gitlab.com/gitlab-org/telemetry/issues/18 for more context.
{% enddocs %}

{% docs zuora_excluded_accounts_doc %}
## [Zuora Excluded Accounts](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/zuora_excluded_accounts.csv)
Zuora Accounts added here will be excluded from all relevant Zuora base models.
* The `is_permanently_excluded` column is non-functional and designates whether the column should be permanently excluded or just temporarily.
* The `description` column is a non-functional helper for us to track which accounts are excluded.
{% enddocs %}

{% docs map_saas_event_to_smau %}

Seed file allowing the Data team to map a SaaS event in `fct_event` and `fct_daily_event` to a SMAU event (in model mart_saas_daily_smau_ and mart_estimated_paid_xmau).

For most of the event, there is a 1:1 relationship between a fct_event event and a SMAU event.

Though, the plan SMAU event is the combination of 2 events (issue_creation and issue_note_creation).

{% enddocs %}

{% docs map_saas_event_to_gmau %}

Seed file allowing the Data team to map a SaaS event in `fct_event` and `fct_daily_event` to a GMAU event (in model mart_saas_daily_smau_ and mart_estimated_paid_xmau).

For most of the event, there is a 1:1 relationship between a fct_event event and a SMAU event.

Though, the plan SMAU event is the combination of 2 events (issue_creation and issue_note_creation).

{% enddocs %}

