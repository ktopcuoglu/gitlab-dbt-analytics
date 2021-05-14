WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'account') }}

), renamed AS (

    SELECT
      id                                            AS account_id,
      name                                          AS account_name,

      -- keys
      account_id_18__c                              AS account_id_18,
      masterrecordid                                AS master_record_id,
      ownerid                                       AS owner_id,
      parentid                                      AS parent_id,
      primary_contact_id__c                         AS primary_contact_id,
      recordtypeid                                  AS record_type_id,
      ultimate_parent_account_id__c                 AS ultimate_parent_id,
      partner_vat_tax_id__c                         AS partner_vat_tax_id,

      -- key people GL side
      federal_account__c                            AS federal_account,
      gitlab_com_user__c                            AS gitlab_com_user,
      account_manager__c                            AS account_manager,
      account_owner_calc__c                         AS account_owner,
      account_owner_team__c                         AS account_owner_team,
      business_development_rep__c                   AS business_development_rep,
      dedicated_service_engineer__c                 AS dedicated_service_engineer,
      sdr_assigned__c                               AS sales_development_rep,
      sdr_account_team__c                           AS sales_development_rep_team,
      -- solutions_architect__c                     AS solutions_architect,
      technical_account_manager_lu__c               AS technical_account_manager_id,

      -- info
      "{{this.database}}".{{target.schema}}.id15to18(substring(regexp_replace(ultimate_parent_account__c,
                     '_HL_ENCODED_/|<a\\s+href="/', ''), 0, 15))
                                                    AS ultimate_parent_account_id,
      type                                          AS account_type,
      dfox_industry__c                              AS df_industry,
      industry                                      AS industry,
      account_tier__c                               AS account_tier,
      customer_since__c::DATE                       AS customer_since_date,
      carr_this_account__c                          AS carr_this_account,
      carr_total__c                                 AS carr_total,
      next_renewal_date__c                          AS next_renewal_date,
      license_utilization__c                        AS license_utilization,
      region__c                                     AS account_region,
      sub_region__c                                 AS account_sub_region,
      support_level__c                              AS support_level,
      named_account__c                              AS named_account,
      billingcountry                                AS billing_country,
      billingpostalcode                             AS billing_postal_code,
      sdr_target_account__c::BOOLEAN                AS is_sdr_target_account,
      potential_arr_lam__c                          AS potential_arr_lam,
      jihu_account__c::BOOLEAN                      AS is_jihu_account,

      -- territory success planning fields
      atam_approved_next_owner__c                   AS tsp_approved_next_owner,
      atam_next_owner_role__c                       AS tsp_next_owner_role,
      atam_next_owner_team__c                       AS tsp_next_owner_team,
      atam_account_employees__c                     AS tsp_account_employees,
      jb_max_family_employees__c                    AS tsp_max_family_employees,
      TRIM(SPLIT_PART(atam_region__c, '-', 1))      AS tsp_region,
      TRIM(SPLIT_PART(atam_sub_region__c, '-', 1))  AS tsp_sub_region,
      TRIM(SPLIT_PART(atam_area__c, '-', 1))        AS tsp_area,
      atam_territory__c                             AS tsp_territory,
      atam_address_country__c                       AS tsp_address_country,
      atam_address_state__c                         AS tsp_address_state,
      atam_address_city__c                          AS tsp_address_city,
      atam_address_street__c                        AS tsp_address_street,
      atam_address_postal_code__c                   AS tsp_address_postal_code,

      -- present state info
      health__c                                     AS health_score,
      gs_health_score__c                            AS health_number,
      gs_health_score_color__c                      AS health_score_color,

      -- opportunity metrics
      count_of_active_subscription_charges__c       AS count_active_subscription_charges,
      count_of_active_subscriptions__c              AS count_active_subscriptions,
      count_of_billing_accounts__c                  AS count_billing_accounts,
      license_user_count__c                         AS count_licensed_users,
      count_of_new_business_won_opps__c             AS count_of_new_business_won_opportunities,
      count_of_open_renewal_opportunities__c        AS count_open_renewal_opportunities,
      count_of_opportunities__c                     AS count_opportunities,
      count_of_products_purchased__c                AS count_products_purchased,
      count_of_won_opportunities__c                 AS count_won_opportunities,
      concurrent_ee_subscriptions__c                AS count_concurrent_ee_subscriptions,
      ce_instances__c                               AS count_ce_instances,
      active_ce_users__c                            AS count_active_ce_users,
      number_of_open_opportunities__c               AS count_open_opportunities,
      using_ce__c                                   AS count_using_ce,

      --account based marketing fields
      abm_tier__c                                   AS abm_tier,
      gtm_strategy__c                               AS gtm_strategy,
      gtm_acceleration_date__c                      AS gtm_acceleration_date,
      gtm_account_based_date__c                     AS gtm_account_based_date,
      gtm_account_centric_date__c                   AS gtm_account_centric_date,
      abm_tier_1_date__c                            AS abm_tier_1_date,
      abm_tier_2_date__c                            AS abm_tier_2_date,
      abm_tier_3_date__c                            AS abm_tier_3_date,

      --demandbase fields
      account_list__c                               AS demandbase_account_list,
      intent__c                                     AS demandbase_intent,
      page_views__c                                 AS demandbase_page_views,
      score__c                                      AS demandbase_score,
      sessions__c                                   AS demandbase_sessions,
      trending_offsite_intent__c                    AS demandbase_trending_offsite_intent,
      trending_onsite_engagement__c                 AS demandbase_trending_onsite_engagement,

      -- sales segment fields
      ultimate_parent_sales_segment_employees__c    AS ultimate_parent_sales_segment,
      sales_segmentation_new__c                     AS division_sales_segment,
      jb_test_sales_segment__c                      AS tsp_max_hierarchy_sales_segment,

      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
      jb_test_sales_segment__c                      AS tsp_test_sales_segment,
      ultimate_parent_sales_segment_employees__c    AS sales_segment,
      sales_segmentation_new__c                     AS account_segment,

      -- ************************************
      -- NF: 2020-12-17
      -- these three fields are used to identify accounts owned by reps within hierarchies that they do not fully own
      -- or even within different regions

      locally_Managed__c                            AS is_locally_managed_account,
      strategic__c                                  AS is_strategic_account,

     -- ************************************
     -- New SFDC Account Fields for FY22 Planning
     next_fy_account_owner_temp__c                  AS next_fy_account_owner_temp,
     next_fy_planning_notes_temp__c                 AS next_fy_planning_notes_temp,
     next_fy_tsp_territory_temp__c                  AS next_fy_tsp_territory_temp,
     next_fy_user_area_temp__c                      AS next_fy_user_area_temp,
     next_fy_user_geo_temp__c                       AS next_fy_user_geo_temp,
     next_fy_user_region_temp__c                    AS next_fy_user_region_temp,
     next_fy_user_segment_temp__c                   AS next_fy_user_segment_temp,

      -- metadata
      createdbyid                                   AS created_by_id,
      createddate                                   AS created_date,
      isdeleted                                     AS is_deleted,
      lastmodifiedbyid                              AS last_modified_by_id,
      lastmodifieddate                              AS last_modified_date,
      lastactivitydate                              AS last_activity_date,
      convert_timezone('America/Los_Angeles',convert_timezone('UTC',current_timestamp())) AS _last_dbt_run,
      systemmodstamp

    FROM source
)

SELECT *
FROM renamed
