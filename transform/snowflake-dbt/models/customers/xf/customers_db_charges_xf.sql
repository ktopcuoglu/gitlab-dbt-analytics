WITH valid_charges AS (
  
    SELECT * 
    FROM {{ ref('customers_db_orders_with_valid_charges_data') }}
  
)

, incomplete_charges AS (
  
    SELECT * 
    FROM {{ ref('customers_db_orders_with_incomplete_charges_data') }}
  
)

, zuora_base_mrr AS (
  
    SELECT * 
    FROM {{ ref('zuora_base_mrr') }}
  
)


, unioned_charges AS (
  
    SELECT *
    FROM valid_charges
    
    UNION 
    
    SELECT *
    FROM incomplete_charges
  
)

, joined_with_base_mrr AS (
  
    SELECT
      unioned_charges.rate_plan_charge_id,
      unioned_charges.subscription_name_slugify,
      unioned_charges.rate_plan_id,
      unioned_charges.is_purchased_through_subscription_portal,
      unioned_charges.current_customer_id,
      unioned_charges.current_gitlab_namespace_id,
      unioned_charges.first_customer_id,
      unioned_charges.is_started_with_trial,
      unioned_charges.trial_start_date,
      
      -- Subscription metadata
      zuora_base_mrr.lineage,
      zuora_base_mrr.oldest_subscription_in_cohort,
      zuora_base_mrr.subscription_start_date,
      zuora_base_mrr.subscription_status,
      
      zuora_base_mrr.effective_start_date,
      zuora_base_mrr.effective_end_date,
      
      -- Product Category Info
      zuora_base_mrr.delivery,
      zuora_base_mrr.product_category,
      
      -- Financial Info
      zuora_base_mrr.mrr,
      zuora_base_mrr.tcv
    FROM unioned_charges
    LEFT JOIN zuora_base_mrr
      ON unioned_charges.rate_plan_charge_id = zuora_base_mrr.rate_plan_charge_id

)

SELECT * 
FROM joined_with_base_mrr
