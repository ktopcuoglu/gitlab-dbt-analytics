WITH
    marketing_contact AS
    (SELECT * 
     FROM {{ref('dim_marketing_contact')}} 
),
    role AS
    (SELECT *
     FROM {{ref('bdg_marketing_contact_role')}}
),
    namespace_lineage AS
    (SELECT *
     FROM {{ref('gitlab_dotcom_namespace_lineage')}}
),
    saas_namespace_subscription AS
    (SELECT *
     FROM {{ref('')}}rheannamistry_prod.common.bdg_namespace_order_subscription_active
),
    self_managed_namespace_subscription AS
    (SELECT *
     FROM rheannamistry_prod.common.bdg_self_managed_order_subscription_active
)
   /* trial_history AS
    (SELECT *
     FROM prep.customers.customers_db_trial_histories_source
),
     free_trials AS
    (
      SELECT * 
     FROM trial_history
     JOIN namespace_lineage ON namespace_lineage.namespace_id = trial_history.gl_namespace_id
     WHERE ultimate_namespace_plan_title = 'Free' AND ultimate_parent_plan_title = 'trial'
)*/
 SELECT Top 100* FROM (
    SELECT DISTINCT 
    marketing_contact.dim_marketing_contact_id,
    marketing_contact.email_address,
    role.namespace_id,    
    CASE WHEN saas_namespace.namespace_type = 'Individual' THEN 1 ELSE 0 END AS is_individual,
    role.customer_db_customer_id AS customer_id,
    role.zuora_billing_contact_id,
    role.zuora_subscription_id,
    CASE WHEN role.marketing_contact_role IN ('Personal Namespace Owner','Group Namespace Owner','Group Namespace Member') THEN saas_namespace.product_tier_name_namespace
         WHEN role.marketing_contact_role IN ('Customer DB Owner') THEN saas_customer.product_tier_name_order   
         WHEN role.marketing_contact_role IN ('Zuora Billing Contact') THEN saas_billing_account.product_tier_name_subscription     
    END                                                                                             AS saas_product_tier,
    CASE WHEN role.marketing_contact_role IN ('Customer DB Owner') THEN self_managed_customer.product_tier_name_order   
         WHEN role.marketing_contact_role IN ('Zuora Billing Contact') THEN self_managed_billing_account.product_tier_name_subscription     
    END                                                                                             AS self_managed_product_tier,
    CASE WHEN saas_namespace.product_tier_name_with_trial = 'SaaS - Trial: Ultimate' 
    OR saas_customer.order_is_trial = TRUE THEN 1 ELSE 0 END          AS is_saas_trial,    
   //was ever trial  OR free_trials.namespace_id IS NOT NULL
    CURRENT_DATE - CAST(saas_namespace.saas_trial_expired_on AS DATE)                                                           AS days_since_saas_trial_ended,    
    CASE WHEN saas_product_tier = 'SaaS - Free' THEN 1 ELSE 0 END                                                                            AS is_saas_free_tier,
    CASE WHEN saas_product_tier = 'SaaS - Bronze' THEN 1 ELSE 0 END                                                                          AS is_saas_bronze_tier,
    CASE WHEN saas_product_tier = 'SaaS - Premium' THEN 1 ELSE 0 END                                                                         AS is_saas_premium_tier,
    CASE WHEN saas_product_tier = 'SaaS - Ultimate' THEN 1 ELSE 0 END                                                                        AS is_saas_ultimate_tier,       
    CASE WHEN self_managed_product_tier = 'Self-Managed - Starter' THEN 1 ELSE 0 END                                                         AS is_Self_Managed_Starter_Tier,
    CASE WHEN self_managed_product_tier = 'Self-Managed - Premium' THEN 1 ELSE 0 END                                                         AS is_Self_Managed_Premium_Tier,
    CASE WHEN self_managed_product_tier = 'Self-Managed - Ultimate' THEN 1 ELSE 0 END                                                        AS is_Self_Managed_Ultimate_Tier
    FROM role 
    INNER JOIN marketing_contact ON marketing_contact.dim_marketing_contact_id = role.dim_marketing_contact_id
    LEFT JOIN saas_namespace_subscription saas_namespace ON saas_namespace.dim_namespace_id = role.namespace_id
    LEFT JOIN saas_namespace_subscription saas_customer ON saas_customer.customer_id = role.customer_db_customer_id
    LEFT JOIN saas_namespace_subscription saas_billing_account ON saas_billing_account.dim_billing_account_id = role.zuora_billing_contact_id   
    LEFT JOIN self_managed_namespace_subscription self_managed_customer ON self_managed_customer.customer_id = role.customer_db_customer_id
    LEFT JOIN self_managed_namespace_subscription self_managed_billing_account ON self_managed_billing_account.dim_billing_account_id = role.zuora_billing_contact_id   
    LEFT JOIN namespace_lineage ON namespace_lineage.namespace_id = role.namespace_id
    --LEFT JOIN free_trials ON free_trials.namespace_id = role.namespace_id 
    )           
