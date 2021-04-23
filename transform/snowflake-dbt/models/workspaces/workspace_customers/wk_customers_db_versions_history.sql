{{ config({
    "materialized": "table"
    })
}}

WITH customers_db_versions AS (

    SELECT *
    FROM {{ref('customers_db_versions_source')}}

), flattened_object AS (

    SELECT 
      *, 
      SUBSTR(value, 1, POSITION(':' in value) -1) AS column_key, 
      TRIM(SUBSTR(value, POSITION(':' in value) + 1, LEN(value))) AS column_value
    FROM customers_db_versions, lateral split_to_table(customers_db_versions.object, '\n')
    WHERE item_type = 'Order' AND SUBSTR(value, 1, POSITION(':' in value) -1) IS NOT NULL

) 

SELECT 
  version_id, 
  item_id,
  created_at,
  MAX(CASE WHEN column_key = 'id' THEN column_value END) AS order_id,
  MAX(CASE WHEN column_key = 'customer_id' THEN column_value END) AS customer_id,
  MAX(CASE WHEN column_key = 'subscription_name' THEN column_value END) AS subscription_name,
  MAX(CASE WHEN column_key = 'subscription_id' THEN column_value END) AS subscription_id,
  MAX(CASE WHEN column_key = 'start_date' THEN column_value END) AS order_start_date,
  MAX(CASE WHEN column_key = 'created_at' THEN TRY_TO_TIMESTAMP(column_value) END) AS order_created_at,
  MAX(CASE WHEN column_key = 'product_rate_plan_id' THEN column_value END) AS product_rate_plan_id,
  MAX(CASE WHEN column_key = 'quantity' THEN column_value END) AS order_quantity,
  MAX(CASE WHEN column_key = 'end_date' THEN column_value END) AS order_end_date,
  MAX(CASE WHEN column_key = 'amendment_type' THEN column_value END) AS amendment_type,
  MAX(CASE WHEN column_key = 'gl_namespace_id' AND column_value <> '' THEN TRY_TO_NUMBER(TRIM(column_value, '\'')) END) AS dim_namespace_id,
  MAX(CASE WHEN column_key = 'gl_namespace_name' THEN column_value END) AS dim_namespace_name,
  MAX(CASE WHEN column_key = 'trial' THEN column_value END)::BOOLEAN AS is_trial,
  MAX(CASE WHEN column_key = 'last_extra_ci_minutes_sync_at' THEN column_value END) AS last_extra_ci_minutes_sync_at,
  MAX(CASE WHEN column_key = 'zuora_account_id' THEN column_value END) AS zuora_account_id,
  MAX(CASE WHEN column_key = 'increased_billing_rate_notified_at' THEN column_value END) AS increased_billing_rate_notified_at,
  MAX(CASE WHEN column_key = 'reconciliation_accepted' THEN column_value END) AS reconciliation_accepted,
  MAX(CASE WHEN column_key = 'billing_rate_last_action' THEN column_value END) AS billing_rate_last_action,
  MAX(CASE WHEN column_key = 'billing_rate_adjusted_at' THEN column_value END) AS billing_rate_adjusted_at
FROM flattened_object
GROUP BY 1,2,3
