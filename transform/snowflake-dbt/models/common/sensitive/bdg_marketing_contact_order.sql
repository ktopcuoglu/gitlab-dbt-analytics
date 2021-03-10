WITH marketing_contact AS (

    SELECT * 
    FROM {{ref('dim_marketing_contact')}} 

), marketing_contact_role AS (
   
    SELECT *
    FROM {{ref('bdg_marketing_contact_role')}}

), namespace_lineage AS (
    
    SELECT *
    FROM {{ref('prep_namespace')}}

), gitlab_namespaces AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespaces_source') }}

), saas_namespace_subscription AS (
    
    SELECT *
    FROM {{ref('bdg_namespace_order_subscription_active')}}

), self_managed_namespace_subscription AS (
    
    SELECT *
    FROM {{ref('bdg_self_managed_order_subscription_active')}}

), final AS (

     SELECT   
      marketing_contact.dim_marketing_contact_id,
      marketing_contact_role.marketing_contact_role,
      marketing_contact.email_address, 
      COALESCE(marketing_contact_role.namespace_id, 
               saas_namespace.dim_namespace_id, 
               saas_customer.dim_namespace_id, 
               saas_billing_account.dim_namespace_id)                                         AS dim_namespace_id,
      gitlab_namespaces.namespace_path,
      CASE 
        WHEN namespace_lineage.namespace_type = 'Individual' 
          THEN 1 
        ELSE 0 
      END                                                                                     AS is_individual_namespace,
      CASE 
        WHEN namespace_lineage.namespace_type = 'Group' 
          THEN 1 
        ELSE 0 
      END                                                                                     AS is_group_namespace,
      marketing_contact_role.customer_db_customer_id                                          AS customer_id,
      marketing_contact_role.zuora_billing_account_id                                         AS dim_billing_account_id,
      CASE
        WHEN saas_namespace.dim_namespace_id IS NOT NULL
          THEN saas_namespace.subscription_start_date
        WHEN saas_customer.dim_namespace_id IS NOT NULL
          THEN saas_customer.subscription_start_date
        WHEN saas_billing_account.dim_namespace_id IS NOT NULL
          THEN saas_billing_account.subscription_start_date
        WHEN self_managed_customer.customer_id IS NOT NULL
          THEN self_managed_customer.subscription_start_date
        WHEN self_managed_billing_account.customer_id IS NOT NULL
          THEN self_managed_billing_account.subscription_start_date
      END                                                                                     AS subscription_start_date,
      CASE
        WHEN saas_namespace.dim_namespace_id IS NOT NULL
          THEN saas_namespace.subscription_end_date
        WHEN saas_customer.dim_namespace_id IS NOT NULL
          THEN saas_customer.subscription_end_date
        WHEN saas_billing_account.dim_namespace_id IS NOT NULL
          THEN saas_billing_account.subscription_end_date
        WHEN self_managed_customer.customer_id IS NOT NULL
          THEN self_managed_customer.subscription_end_date
        WHEN self_managed_billing_account.customer_id IS NOT NULL
          THEN self_managed_billing_account.subscription_end_date
      END                                                                                     AS subscription_end_date,
      CASE 
        WHEN marketing_contact_role.namespace_id IS NOT NULL 
          AND saas_namespace.product_tier_name_namespace is NULL
          THEN 'SaaS - Free' 
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Personal Namespace Owner'
                                                                , 'Group Namespace Owner'
                                                                , 'Group Namespace Member'
                                                              ) 
          THEN saas_namespace.product_tier_name_namespace
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Customer DB Owner'
                                                              ) 
          THEN saas_customer.product_tier_name_with_trial   
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Zuora Billing Contact'
                                                              ) 
          THEN saas_billing_account.product_tier_name_subscription     
      END                                                                                     AS saas_product_tier,
      CASE 
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Customer DB Owner'
                                                              ) 
          THEN self_managed_customer.product_tier_name_with_trial   
        WHEN marketing_contact_role.marketing_contact_role IN (
                                                                'Zuora Billing Contact'
                                                              ) 
          THEN self_managed_billing_account.product_tier_name_subscription     
      END                                                                                     AS self_managed_product_tier,
      CASE 
        WHEN saas_namespace.product_tier_name_with_trial = 'SaaS - Trial: Ultimate' 
          OR saas_customer.order_is_trial = TRUE 
          THEN 1 
        ELSE 0 
      END                                                                                     AS is_saas_trial,    
      CURRENT_DATE - CAST(saas_namespace.saas_trial_expired_on AS DATE)                       AS days_since_saas_trial_ended,    
      CASE 
        WHEN saas_product_tier = 'SaaS - Free' 
          THEN 1
        ELSE 0
      END                                                                                     AS is_saas_free_tier,
      CASE 
        WHEN saas_product_tier = 'SaaS - Bronze' THEN 1 
        ELSE 0 
      END                                                                                     AS is_saas_bronze_tier,
      CASE 
        WHEN saas_product_tier = 'SaaS - Premium' THEN 1 
        ELSE 0 
      END                                                                                     AS is_saas_premium_tier,
      CASE 
        WHEN saas_product_tier = 'SaaS - Ultimate' THEN 1 
        ELSE 0 
      END                                                                                     AS is_saas_ultimate_tier,       
      CASE 
        WHEN self_managed_product_tier = 'Self-Managed - Starter' THEN 1 
        ELSE 0 
      END                                                                                     AS is_self_managed_starter_tier,
      CASE 
        WHEN self_managed_product_tier = 'Self-Managed - Premium' THEN 1 
        ELSE 0 
      END                                                                                     AS is_self_managed_premium_tier,
      CASE 
        WHEN self_managed_product_tier = 'Self-Managed - Ultimate' THEN 1 
        ELSE 0 
      END                                                                                     AS is_self_managed_ultimate_tier
    
    FROM marketing_contact_role 
    INNER JOIN marketing_contact 
      ON marketing_contact.dim_marketing_contact_id = marketing_contact_role.dim_marketing_contact_id
    LEFT JOIN saas_namespace_subscription saas_namespace 
      ON saas_namespace.dim_namespace_id = marketing_contact_role.namespace_id
    LEFT JOIN saas_namespace_subscription saas_customer 
      ON saas_customer.customer_id = marketing_contact_role.customer_db_customer_id
    LEFT JOIN saas_namespace_subscription saas_billing_account 
      ON saas_billing_account.dim_billing_account_id = marketing_contact_role.zuora_billing_account_id   
    LEFT JOIN self_managed_namespace_subscription self_managed_customer 
      ON self_managed_customer.customer_id = marketing_contact_role.customer_db_customer_id
    LEFT JOIN self_managed_namespace_subscription self_managed_billing_account 
      ON self_managed_billing_account.dim_billing_account_id = marketing_contact_role.zuora_billing_account_id   
    LEFT JOIN namespace_lineage 
      ON namespace_lineage.namespace_id = COALESCE(marketing_contact_role.namespace_id,
                                                   saas_namespace.dim_namespace_id,
                                                   saas_customer.dim_namespace_id,
                                                   saas_billing_account.dim_namespace_id)
    left join gitlab_namespaces 
      ON gitlab_namespaces.namespace_id = namespace_lineage.namespace_id
      
    )    
    

{{ dbt_audit(
    cte_ref="final",
    created_by="@trevor31",
    updated_by="@trevor31",
    created_date="2021-02-04",
    updated_date="2021-03-10"
) }}
