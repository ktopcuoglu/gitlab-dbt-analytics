/* grain: one record per subscription per month */
WITH mrr AS (

    SELECT
      mrr_id,
      dim_date_id,
      dim_billing_account_id,
      dim_crm_account_id,
      dim_subscription_id,
      dim_product_detail_id,
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
    updated_date="2021-01-21",
) }}
