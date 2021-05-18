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
