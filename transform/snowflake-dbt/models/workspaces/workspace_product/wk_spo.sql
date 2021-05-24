{{ config({
    "materialized": "table"
    })
}}

{{simple_cte([
    ('wk_saas_spo', 'wk_saas_spo'), 
    ('wk_self_managed_spo', 'wk_self_managed_spo')
])
}}

SELECT 
  reporting_month,
  organization_id,
  delivery,
  organization_type,
  product_tier,
  is_paid_product_tier,
  umau_value,
  configure_stage,
  create_stage,
  manage_stage,
  monitor_stage,
  package_stage,
  plan_stage,
  protect_stage,
  release_stage,
  secure_stage,
  verify_stage
FROM wk_self_managed_spo

UNION

SELECT 
  reporting_month,
  organization_id,
  delivery,
  organization_type,
  product_tier,
  is_paid_product_tier,
  umau_value,
  configure_stage,
  create_stage,
  manage_stage,
  monitor_stage,
  package_stage,
  plan_stage,
  protect_stage,
  release_stage,
  secure_stage,
  verify_stage
FROM wk_saas_spo
