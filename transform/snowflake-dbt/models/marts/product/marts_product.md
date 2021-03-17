{% docs mart_estimated_smau %}

Data mart to explore SMAU. The report looks at the usage ping sent by instances using GitLab. Then, for each stage, the report looks at the specific metrics/counter which is chosen to represent SMAU values. It then calculates Recorded SMAU.

Then calculates the Estimated SMAU Value as explained in detail in this page.

```
SELECT
  arr_month,
  SUM(arr)  AS arr
FROM "PROD"."LEGACY"."MART_ARR"
WHERE arr_month < DATE_TRUNC('month',CURRENT_DATE)
GROUP BY 1
ORDER BY 1 DESC
```

ERD explaining the logic coming soon

{% enddocs %}
