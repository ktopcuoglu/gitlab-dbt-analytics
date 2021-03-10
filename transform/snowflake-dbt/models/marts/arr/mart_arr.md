{% docs mart_arr %}

Data mart to explore ARR. This model is built using the same logic as the Zuora UI out of the box MRR Trend Report. The report looks at the charges associated with subscriptions, along with their effective dates and subscription statuses, and calculates ARR.

The below query will pull ARR by month. You can add additional dimensions to the query to build out your analysis.

SELECT
  arr_month,
  SUM(arr)  AS arr
FROM "PROD"."LEGACY"."MART_ARR"
WHERE arr_month < DATE_TRUNC('month',CURRENT_DATE)
GROUP BY 1
ORDER BY 1 DESC

Charges_month_by_month CTE:

This CTE amortizes the ARR by month over the effective term of the rate plan charges. There are 4 subscription statuses in Zuora: active, cancelled, draft and expired. The Zuora UI reporting modules use a filter of WHERE subscription_status NOT IN ('Draft','Expired') which is also applied in this query. Please see the column definitions for additional details.

Here is an image documenting the ERD for this table:

<div style="width: 640px; height: 480px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:640px; height:480px" src="https://app.lucidchart.com/documents/embeddedchart/998dbbae-f04e-4310-9d85-0c360a40a018" id="T0XuoGn786sQ"></iframe></div>

{% enddocs %}

{% docs mart_arr_snapshots %}

Keeps daily snapshots of mart_arr. This allows to query ARR from a historical perspective. 

The below query will pull ARR by month as observed on selected snapshot_date.

SELECT
  arr_month,
  SUM(arr)  AS arr
FROM "PROD"."LEGACY"."MART_ARR_SNAPSHOTS"
WHERE arr_month < DATE_TRUNC('month',CURRENT_DATE)
GROUP BY 1
ORDER BY 1 DESC
WHERE snapshot_date = '2020-08-01'

{% enddocs %}

{% enddocs %}

{% docs mart_arr_col_months_since_parent_cohort_start %}

The number of months between the MRR being reported in that row and the parent account cohort month. Must be a positive number.

{% enddocs %}

{% docs mart_arr_col_quarters_since_parent_cohort_start %}

The number of quarters between the MRR being reported in that row and the parent account cohort quarter. Must be a positive number.

{% enddocs %}

{% docs mart_arr_col_parent_account_cohort_quarter %}

The cohort quarter of the ultimate parent account.

{% enddocs %}


{% docs mart_arr_col_parent_account_cohort_month %}

The cohort month of the ultimate parent account.

{% enddocs %}