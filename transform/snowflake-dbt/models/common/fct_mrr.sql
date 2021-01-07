/* grain: one record per subscription per month */
{{config({
    "schema": "legacy"
  })
}}

WITH mrr AS (

    SELECT
      mrr_id,
      date_id,
      billing_account_id,
      crm_account_id,
      subscription_id,
      product_details_id,
      mrr,
      arr,
      quantity,
      unit_of_measure
    FROM {{ ref('prep_recurring_charge') }}
    WHERE mrr != 0 /* This excludes Education customers (charge name EDU or OSS) with free subscriptions */
)

{{ dbt_audit(
    cte_ref="mrr",
    created_by="@msendal",
    updated_by="@mcooperDD",
    created_date="2020-09-10",
    updated_date="2021-01-04",
) }}
