WITH marketing_contact AS (

    SELECT * 
    FROM {{ref('dim_marketing_contact')}}

), marketing_contact_role AS (
    
    SELECT * 
    FROM {{ref('bdg_marketing_contact_role')}}

), marketing_contact_order AS (
  
    SELECT * 
    FROM {{ref('bdg_marketing_contact_order')}}

), prep AS (
  
    SELECT      
      marketing_contact.dim_marketing_contact_id,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_role.marketing_contact_role = 'Group Namespace Owner' THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_group_namespace_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_role.marketing_contact_role = 'Group Namespace Member' THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_group_namespace_member,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_role.marketing_contact_role = 'Personal Namespace Owner' THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_personal_namespace_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_role.marketing_contact_role = 'Customer DB Owner' THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_customer_db_owner,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_role.marketing_contact_role = 'Zuora Billing Contact' THEN 1 
                  ELSE 0 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_zuora_billing_contact,
      MIN(marketing_contact_order.days_since_saas_trial_ended)                                   AS days_since_saas_trial_ended,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 1 THEN is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_is_saas_trial,   
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 1 THEN is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_is_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 1 THEN is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_is_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 1 THEN is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_is_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 1 THEN is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS individual_is_saas_ultimate_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_is_saas_trial,      
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_is_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_member_is_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_member_is_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Member'
                    THEN is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_member_is_saas_ultimate_tier,      
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Owner'
                    THEN is_saas_trial 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_is_saas_trial,    
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Owner'
                    THEN is_saas_free_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_is_saas_free_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Owner'
                    THEN is_saas_bronze_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_owner_is_saas_bronze_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Owner'
                    THEN is_saas_premium_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS group_owner_is_saas_premium_tier,
      CASE 
        WHEN MAX(CASE 
                  WHEN marketing_contact_order.is_individual = 0 AND marketing_contact_role.marketing_contact_role = 'Group Namespace Owner'
                    THEN is_saas_ultimate_tier 
                  ELSE NULL 
                END) >= 1 THEN TRUE 
        ELSE FALSE
      END                                                                                        AS group_owner_is_saas_ultimate_tier,      
      CASE 
        WHEN MAX(is_self_managed_starter_tier)  >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_starter_tier, 
      CASE 
        WHEN MAX(is_self_managed_premium_tier)  >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_premium_tier, 
      CASE 
        WHEN MAX(is_self_managed_ultimate_tier) >= 1 THEN TRUE 
        ELSE FALSE 
      END                                                                                        AS is_self_managed_ultimate_tier   
    FROM marketing_contact
    LEFT JOIN  marketing_contact_order
      ON marketing_contact_order.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    LEFT JOIN marketing_contact_role
      ON marketing_contact_role.dim_marketing_contact_id = marketing_contact.dim_marketing_contact_id
    GROUP BY marketing_contact.dim_marketing_contact_id

), final AS (

    SELECT 
      prep.*, 
      marketing_contact.email_address,
      marketing_contact.first_name,
      marketing_contact.last_name,
      marketing_contact.gitlab_user_name,
      marketing_contact.company_name,
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
      marketing_contact.is_customer_db_user,
      marketing_contact.customer_db_customer_id,
      marketing_contact.customer_db_created_date,
      marketing_contact.customer_db_confirmed_date,
      marketing_contact.zuora_contact_id,
      marketing_contact.zuora_created_date,
      marketing_contact.zuora_active_state
    FROM prep
    LEFT JOIN marketing_contact 
      ON marketing_contact.dim_marketing_contact_id = prep.dim_marketing_contact_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@trevor31",
    updated_by="@trevor31",
    created_date="2021-02-09",
    updated_date="2021-02-09"
) }}


