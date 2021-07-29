{% docs mart_ci_runner_activity_monthly %}

Mart table containing quantitative data related to CI runner activity on GitLab.com, combined with associated CI meta data.

These metrics are aggregated at a monthly grain.

{% enddocs %}

{% docs mart_estimated_xmau %}

Data mart to explore SMAU. The report looks at the usage ping sent by instances using GitLab. Then, for each stage, the report looks at the specific metrics/counter which is chosen to represent SMAU values. It then calculates Recorded SMAU.

Then calculates the Estimated SMAU Value as explained in detail in this page.

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
