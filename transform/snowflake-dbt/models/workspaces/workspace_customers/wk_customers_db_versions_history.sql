{{ config({
    "materialized": "table"
    })
}}

WITH customers_db_versions AS (

    SELECT *
    FROM {{ref('customers_db_versions_source')}}
    -- selecting only orders
    WHERE item_type = 'Order'

), flattened_object AS (

    SELECT 
      *, 
      SUBSTR(value, 1, POSITION(':' in value) -1) AS column_key, 
      TRIM(SUBSTR(value, POSITION(':' in value) + 1, LEN(value))) AS column_value
    FROM customers_db_versions,
      -- objects look like a yaml table, splitting the object into rows at each linebreak 
      lateral split_to_table(customers_db_versions.object, '\n')
    WHERE item_type = 'Order' AND SUBSTR(value, 1, POSITION(':' in value) -1) IS NOT NULL

), old_changes AS (

    SELECT 
      version_id, 
      item_id,
      created_at AS valid_to,
      MAX(CASE WHEN column_key = 'id' THEN TRY_TO_NUMBER(column_value) END)                                                 AS order_id,
      MAX(CASE WHEN column_key = 'customer_id' THEN TRY_TO_NUMBER(column_value) END)                                        AS customer_id,
      MAX(CASE WHEN column_key = 'subscription_name' THEN column_value END)                                                 AS subscription_name,
      MAX(CASE WHEN column_key = 'subscription_id' THEN column_value END)                                                   AS subscription_id,
      MAX(CASE WHEN column_key = 'start_date' THEN TRY_TO_DATE(column_value) END)::TIMESTAMP                                AS order_start_date,
      MAX(CASE WHEN column_key = 'created_at' THEN TRY_TO_TIMESTAMP(column_value) END)::TIMESTAMP                           AS order_created_at,
      MAX(CASE WHEN column_key = 'product_rate_plan_id' THEN column_value END)                                              AS product_rate_plan_id,
      MAX(CASE WHEN column_key = 'quantity' THEN TRY_TO_NUMBER(column_value) END)::INTEGER                                  AS order_quantity,
      MAX(CASE WHEN column_key = 'end_date' THEN TRY_TO_DATE(column_value) END)::TIMESTAMP                                  AS order_end_date,
      MAX(CASE WHEN column_key = 'amendment_type' THEN column_value END)                                                    AS amendment_type,
      MAX(CASE WHEN column_key = 'gl_namespace_id' AND column_value <> '' THEN TRY_TO_NUMBER(TRIM(column_value, '\'')) END) AS dim_namespace_id,
      MAX(CASE WHEN column_key = 'gl_namespace_name' THEN column_value END)                                                 AS dim_namespace_name,
      MAX(CASE WHEN column_key = 'trial' THEN column_value END)::BOOLEAN                                                    AS order_is_trial,
      MAX(CASE WHEN column_key = 'last_extra_ci_minutes_sync_at' THEN TRY_TO_TIMESTAMP(column_value) END)                   AS last_extra_ci_minutes_sync_at,
      MAX(CASE WHEN column_key = 'zuora_account_id' THEN column_value END)                                                  AS zuora_account_id,
      MAX(CASE WHEN column_key = 'increased_billing_rate_notified_at' THEN TRY_TO_TIMESTAMP(column_value) END)              AS increased_billing_rate_notified_at,
      MAX(CASE WHEN column_key = 'reconciliation_accepted' THEN column_value END)                                           AS reconciliation_accepted,
      MAX(CASE WHEN column_key = 'billing_rate_last_action' THEN column_value END)                                          AS billing_rate_last_action,
      MAX(CASE WHEN column_key = 'billing_rate_adjusted_at' THEN TRY_TO_TIMESTAMP(column_value) END)::TIMESTAMP             AS billing_rate_adjusted_at
    FROM flattened_object
    GROUP BY 1,2,3

), transformed AS (

    SELECT 
      customers_db_versions.item_id AS order_from_source, 
      old_changes.*, 
      COALESCE(LAG(customers_db_versions.created_at) OVER (PARTITION BY old_changes.order_id ORDER BY customers_db_versions.version_id ASC), old_changes.order_created_at) AS valid_from
    FROM customers_db_versions
    LEFT JOIN old_changes ON customers_db_versions.version_id = old_changes.version_id

), unioned AS (

    SELECT 
      order_id,
      customer_id,
      product_rate_plan_id,
      subscription_id,
      subscription_name,
      order_start_date,
      order_end_date,
      order_quantity,
      order_created_at,
      dim_namespace_id,
      dim_namespace_name,
      amendment_type,
      order_is_trial,
      last_extra_ci_minutes_sync_at,
      zuora_account_id,
      increased_billing_rate_notified_at,
      valid_from,
      valid_to
    FROM transformed
    WHERE order_created_at IS NOT NULL

    UNION 

    SELECT 
    order_id,
    customer_id,
    product_rate_plan_id,
    subscription_id,
    subscription_name,
    order_start_date,
    order_end_date,
    order_quantity,
    order_created_at AS valid_to,
    gitlab_namespace_id,
    gitlab_namespace_name,
    amendment_type,
    order_is_trial,
    last_extra_ci_minutes_sync_at,
    zuora_account_id,
    increased_billing_rate_notified_at,
    order_updated_at AS valid_from,
    NULL AS valid_to
    FROM {{ref('customers_db_orders_source')}}

)

SELECT *
FROM unioned
