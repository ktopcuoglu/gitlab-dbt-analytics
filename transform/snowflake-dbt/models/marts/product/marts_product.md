{% docs mart_ci_runner_activity_monthly %}

Mart table containing quantitative data related to CI runner activity on GitLab.com.

These metrics are aggregated at a monthly grain per `dim_namespace_id`.

Additional identifier/key fields - `dim_ci_runner_id`, `dim_ci_pipeline_id`, `dim_ci_stage_id` have been included for Reporting purposes. 

Only activity since 2020-01-01 is being processed due to the high volume of the data.

{% enddocs %}

{% docs mart_estimated_xmau %}

Documentation around Estimated [xMAU methodology is explained here](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html)

Data mart to explore estimated xMAU PIs. The report looks at the usage ping sent by instances using GitLab. Then, for each stage, the report looks at the specific metrics/counter which is chosen to represent SMAU values. It then calculates Recorded SMAU.

Then calculates the Estimated SMAU Value as explained [in detail in this page](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html).

```
SELECT
  reporting_month,
  stage_name,
  SUM(estimated_monthly_metric_value_sum)  AS xmau
FROM "PROD"."LEGACY"."MART_ESTIMATED_XMAU"
WHERE xmau_level = 'SMAU'
GROUP BY 1,2
ORDER BY 1 DESC
```

ERD explaining the logic coming soon

{% enddocs %}

{% docs mart_estimated_paid_xmau %}

Documentation around Estimated [xMAU methodology is explained here](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html)

Data mart to explore estimated paid xMAU PIs. The report looks at the usage ping sent by instances using GitLab. Then, for each stage, the report looks at the specific metrics/counter which is chosen to represent SMAU values. It then calculates Recorded SMAU.

Then calculates the Estimated SMAU Value as explained [in detail in this page](https://about.gitlab.com/handbook/business-technology/data-team/data-catalog/xmau-analysis/estimation-xmau-algorithm.html).

```
SELECT
  reporting_month,
  stage_name,
  SUM(estimated_monthly_metric_value_sum)  AS xmau
FROM "PROD"."LEGACY"."MART_ESTIMATED_XMAU"
WHERE xmau_level = 'SMAU'
GROUP BY 1,2
ORDER BY 1 DESC
```

ERD explaining the logic coming soon

{% enddocs %}

{% docs mart_product_usage_free_user_metrics_monthly %}
This table unions the sets of all Self-Managed and SaaS **free users**. The data from this table will be used for  Customer Product Insights by Sales team.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_saas_daily_smau_events_400 %}

Mart table built on top of `fct_daily_event_400` (hence this table shows only the last 400 days) that allows final user to easily calculate some key Performance Indicators like Monthly SMAU (paid or free) and allowing complex analysis like:

1. [User Journey Analysis](https://app.periscopedata.com/app/gitlab/869174/WIP-Cross-Stage-Adoption-Dashboard): See how often different product stages are used by the same namespaces. See what stages are used in combination.
1. [New Namespace Stage Adoption](https://app.periscopedata.com/app/gitlab/761347/Group-Namespace-Conversion-Metrics): Evaluate how often new namespaces are adopting stages such as 'Create' and 'Verify' within their first days of use.
1. [Stages per Organization](https://app.periscopedata.com/app/gitlab/824044/Stages-per-Organization-Deep-Dive---SpO): Identify how namespaces adopt stages within their first days and how this correlates with paid conversion and long-term engagement.

All these analysis have been done with `gitlab_dotcom_daily_usage_data_events` which was the legacy table of these tables.
Only events that happened the last 400 days are included in this table.
{% enddocs %}

{% docs mart_ci_runner_activity_daily %}
 
Mart table containing quantitative data related to CI runner activity on GitLab.com.
 
These metrics are aggregated at a daily grain per `dim_project_id`.

Additional identifier/key fields - `dim_ci_runner_id`, `dim_ci_pipeline_id`, `dim_ci_stage_id` have been included for Reporting purposes. 

Only activity since 2020-01-01 is being processed due to the high volume of the data.

{% enddocs %}

{% docs mart_user_request %}
 
Mart table that contains all user requests to the Gitlab product by the customers.
 
It unions `bdg_issue_user_request` and `bdg_epic_user_request` to have the product request that are contained both in the epics and issues in the `gitlab-org` group.
After that, it adds useful data around these issues and epics as well the crm_account and crm_opportunity links that are useful to prioritize those user requests.

{% enddocs %}