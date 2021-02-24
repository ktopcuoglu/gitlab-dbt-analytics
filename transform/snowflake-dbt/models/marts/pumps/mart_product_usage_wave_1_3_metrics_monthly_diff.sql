{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('monthly_metrics','fct_product_usage_wave_1_3_metrics_monthly'),
    ('billing_accounts','dim_billing_account'),
    ('crm_accounts','dim_crm_account')
]) }}

, joined AS (

    SELECT 1=1

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-02-16",
    updated_date="2021-02-16"
) }}