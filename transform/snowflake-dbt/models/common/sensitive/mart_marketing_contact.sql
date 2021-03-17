WITH marketing_contact AS (

    SELECT * 
    FROM {{ref('dim_marketing_contact')}}

), marketing_contact_order AS (
  
    SELECT * 
    FROM {{ref('bdg_marketing_contact_order')}}

), subscription_aggregate AS (

    SELECT 
      dim_marketing_contact_id,
      MIN(subscription_start_date)                                                               AS min_subscription_start_date,
      MAX(subscription_end_date)                                                                 AS max_subscription_end_date,
      COUNT(*)                                                                                   AS nbr_of_subscriptions
    FROM marketing_contact_order
    WHERE subscription_start_date is not null
    GROUP BY dim_marketing_contact_id

), paid_subscription_aggregate AS (

    SELECT 
      dim_marketing_contact_id,
      COUNT(DISTINCT dim_subscription_id)                                                        AS nbr_of_paid_subscriptions
    FROM marketing_contact_order
    WHERE dim_subscription_id is not null
      AND (is_saas_bronze_tier 
           OR is_saas_premium_tier 
           OR is_saas_ultimate_tier 
           OR is_self_managed_starter_tier
           OR is_self_managed_premium_tier
           OR is_self_managed_ultimate_tier
          )
    GROUP BY dim_marketing_contact_id

), prep AS (
  
    SELECT     
      marketing_contact.dim_marketing_contact_id,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Group Namespace Owner' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_group_namespace_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Group Namespace Member' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_group_namespace_member,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Personal Namespace Owner' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_individual_namespace_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Customer DB Owner' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_customer_db_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.marketing_contact_role = 'Zuora Billing Contact' 
                    THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_zuora_billing_contact,
      MIN(marketing_contact_order.days_since_saas_trial_ended)                                   AS days_since_saas_trial_ended,
      MAX(marketing_contact_order.days_until_saas_trial_ends)                                         AS days_until_saas_trial_ends,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_trial,   
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 1 
                    THEN is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_namespace_is_saas_ultimate_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_of_saas_trial,      
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1 
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_of_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1 
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_of_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_group_namespace = 1 
                    AND marketing_contact_order.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_member_of_saas_ultimate_tier,      
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         ) 
                    THEN is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_of_saas_trial,    
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_of_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0 
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_of_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Group Namespace Owner'
                                                                         )
                    THEN is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_owner_of_saas_ultimate_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         ) 
                    THEN is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS responsible_for_group_saas_trial,    
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS responsible_for_group_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0 
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS responsible_for_group_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual_namespace = 0
                    AND marketing_contact_order.marketing_contact_role IN (
                                                                          'Customer DB Owner'
                                                                          , 'Zuora Billing Contact'
                                                                         )
                    THEN is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS responsible_for_group_saas_ultimate_tier,      
      CASE 
        WHEN MAX(is_self_managed_starter_tier)  >= 1 
          THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_starter_tier, 
      CASE 
        WHEN MAX(is_self_managed_premium_tier)  >= 1 
          THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_premium_tier, 
      CASE 
        WHEN MAX(is_self_managed_ultimate_tier) >= 1 
          THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_ultimate_tier,
      ARRAY_AGG(
                DISTINCT IFNULL(marketing_contact_order.marketing_contact_role || ': ' || 
                  IFNULL(marketing_contact_order.saas_product_tier, '') || IFNULL(marketing_contact_order.self_managed_product_tier, ''), 'No Role') 
               )                                                                                 AS role_tier_text,
      ARRAY_AGG(
                DISTINCT IFNULL(marketing_contact_role || ': ' || 
                  IFNULL(namespace_path, CASE 
                                          WHEN self_managed_product_tier IS NOT NULL
                                            THEN 'Self-Managed' 
                                          ELSE '' 
                                        END)  || ' | ' || 
                  IFNULL(saas_product_tier, '') || 
                  IFNULL(self_managed_product_tier, ''), 'No Namespace')
               )                                                                                 AS role_tier_namespace_text

    FROM marketing_contact
    LEFT JOIN  marketing_contact_order
      ON marketing_contact_order.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    GROUP BY marketing_contact.dim_marketing_contact_id

), joined AS (

    SELECT 
      prep.*, 
      subscription_aggregate.min_subscription_start_date,
      subscription_aggregate.max_subscription_end_date,
      paid_subscription_aggregate.nbr_of_paid_subscriptions,
      CASE 
        WHEN (responsible_for_group_saas_free_tier
              OR individual_namespace_is_saas_free_tier
              OR group_owner_of_saas_free_tier
             ) 
             AND NOT (responsible_for_group_saas_ultimate_tier
                      OR responsible_for_group_saas_premium_tier
                      OR responsible_for_group_saas_bronze_tier
                      OR individual_namespace_is_saas_bronze_tier
                      OR individual_namespace_is_saas_premium_tier
                      OR individual_namespace_is_saas_ultimate_tier
                      OR group_owner_of_saas_bronze_tier
                      OR group_owner_of_saas_premium_tier
                      OR group_owner_of_saas_ultimate_tier
                     )
          THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS responsible_for_free_tier_only,
      marketing_contact.email_address,
      marketing_contact.first_name,
      IFNULL(marketing_contact.last_name, 'Unknown')                                             AS last_name,
      marketing_contact.gitlab_user_name,
      IFNULL(marketing_contact.company_name, 'Unknown')                                          AS company_name,
      marketing_contact.job_title,
      marketing_contact.country,
      marketing_contact.sfdc_parent_sales_segment,
      marketing_contact.is_sfdc_lead_contact,
      marketing_contact.sfdc_lead_contact,
      marketing_contact.sfdc_created_date,
      marketing_contact.is_sfdc_opted_out,
      marketing_contact.is_gitlab_dotcom_user,
      marketing_contact.gitlab_dotcom_user_id,
      marketing_contact.gitlab_dotcom_created_date,
      marketing_contact.gitlab_dotcom_confirmed_date,
      marketing_contact.gitlab_dotcom_active_state,
      marketing_contact.gitlab_dotcom_last_login_date,
      marketing_contact.gitlab_dotcom_email_opted_in,
      marketing_contact.days_since_saas_signup,
      marketing_contact.is_customer_db_user,
      marketing_contact.customer_db_customer_id,
      marketing_contact.customer_db_created_date,
      marketing_contact.customer_db_confirmed_date,
      marketing_contact.days_since_self_managed_owner_signup,
      marketing_contact.zuora_contact_id,
      marketing_contact.zuora_created_date,
      marketing_contact.zuora_active_state,
      'Raw'                                                                                      AS lead_status,
      'Snowflake Email Marketing Database'                                                       AS lead_source
    FROM prep
    LEFT JOIN marketing_contact 
      ON marketing_contact.dim_marketing_contact_id = prep.dim_marketing_contact_id
    LEFT JOIN subscription_aggregate
      ON subscription_aggregate.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    LEFT JOIN paid_subscription_aggregate
      ON paid_subscription_aggregate.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id

)

{{ hash_diff(
    cte_ref="joined",
    return_cte="final",
    columns=[
      'is_group_namespace_owner',
      'is_group_namespace_member',
      'is_individual_namespace_owner',
      'is_customer_db_owner',
      'is_zuora_billing_contact',
      'days_since_saas_trial_ended',
      'days_until_saas_trial_ends',
      'individual_namespace_is_saas_trial',
      'individual_namespace_is_saas_free_tier',
      'individual_namespace_is_saas_bronze_tier',
      'individual_namespace_is_saas_premium_tier',
      'individual_namespace_is_saas_ultimate_tier',
      'group_member_of_saas_trial',
      'group_member_of_saas_free_tier',
      'group_member_of_saas_bronze_tier',
      'group_member_of_saas_premium_tier',
      'group_member_of_saas_ultimate_tier',
      'group_owner_of_saas_trial',
      'group_owner_of_saas_free_tier',
      'group_owner_of_saas_bronze_tier',
      'group_owner_of_saas_premium_tier',
      'group_owner_of_saas_ultimate_tier',
      'responsible_for_group_saas_trial',
      'responsible_for_group_saas_free_tier',
      'responsible_for_group_saas_bronze_tier',
      'responsible_for_group_saas_premium_tier',
      'responsible_for_group_saas_ultimate_tier',
      'is_self_managed_starter_tier',
      'is_self_managed_premium_tier',
      'is_self_managed_ultimate_tier',
      'min_subscription_start_date',
      'max_subscription_end_date',
      'nbr_of_paid_subscriptions',
      'email_address',
      'first_name',
      'last_name',
      'gitlab_user_name',
      'company_name',
      'job_title',
      'country',
      'sfdc_parent_sales_segment',
      'is_sfdc_lead_contact',
      'sfdc_lead_contact',
      'sfdc_created_date',
      'is_sfdc_opted_out',
      'is_gitlab_dotcom_user',
      'gitlab_dotcom_user_id',
      'gitlab_dotcom_created_date',
      'gitlab_dotcom_confirmed_date',
      'gitlab_dotcom_active_state',
      'gitlab_dotcom_last_login_date',
      'gitlab_dotcom_email_opted_in',
      'is_customer_db_user',
      'customer_db_customer_id',
      'customer_db_created_date',
      'customer_db_confirmed_date',
      'zuora_contact_id',
      'zuora_created_date',
      'zuora_active_state'
      ]
) }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@trevor31",
    updated_by="@trevor31",
    created_date="2021-02-09",
    updated_date="2021-03-16"
) }}


