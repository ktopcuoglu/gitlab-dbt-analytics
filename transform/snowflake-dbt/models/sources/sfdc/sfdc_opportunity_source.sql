WITH source AS (

    SELECT
      opportunity.*,
      CASE
        WHEN stagename = '0-Pending Acceptance'     THEN x0_pending_acceptance_date__c
        WHEN stagename = '1-Discovery'              THEN x1_discovery_date__c
        WHEN stagename = '2-Scoping'                THEN x2_scoping_date__c
        WHEN stagename = '3-Technical Evaluation'   THEN x3_technical_evaluation_date__c
        WHEN stagename = '4-Proposal'               THEN x4_proposal_date__c
        WHEN stagename = '5-Negotiating'            THEN x5_negotiating_date__c
        WHEN stagename = '6-Awaiting Signature'     THEN x6_awaiting_signature_date__c
      END calculation_days_in_stage_date,
      DATEDIFF(
        days,
        calculation_days_in_stage_date::DATE,
        CURRENT_DATE::DATE
      ) + 1                                         AS days_in_stage
    FROM {{ source('salesforce', 'opportunity') }}  AS opportunity

), renamed AS (

      SELECT
        -- keys
        accountid                                   AS account_id,
        id                                          AS opportunity_id,
        duplicate_opportunity__c                    AS duplicate_opportunity_id,
        name                                        AS opportunity_name,
        ownerid                                     AS owner_id,

        -- logistical information
        isclosed                                    AS is_closed,
        iswon                                       AS is_won,
        closedate                                   AS close_date,
        createddate                                 AS created_date,
        days_in_stage                               AS days_in_stage,
        deployment_preference__c                    AS deployment_preference,
        sql_source__c                               AS generated_source,
        leadsource                                  AS lead_source,
        merged_opportunity__c                       AS merged_opportunity_id,
        account_owner__c                            AS account_owner,
        opportunity_owner__c                        AS opportunity_owner,
        owner_team_o__c                             AS opportunity_owner_team,
        manager_current__c                          AS opportunity_owner_manager,
        sales_market__c                             AS opportunity_owner_department,
        SDR_LU__c                                   AS opportunity_sales_development_representative,
        BDR_LU__c                                   AS opportunity_business_development_representative,
        BDR_SDR__c                                  AS opportunity_development_representative,
        account_owner_team_o__c                     AS account_owner_team_stamped,

        sales_accepted_date__c                      AS sales_accepted_date,
        engagement_type__c                          AS sales_path,
        sales_qualified_date__c                     AS sales_qualified_date,

        type                                        AS sales_type,
        {{  sfdc_source_buckets('leadsource') }}
        stagename                                   AS stage_name,
        revenue_type__c                             AS order_type,
        deal_path__c                                AS deal_path,

        -- opportunity information
        acv_2__c                                    AS acv,
        amount                                      AS amount,
        IFF(acv_2__c >= 0, 1, 0)                    AS closed_deals, -- so that you can exclude closed deals that had negative impact
        competitors__c                              AS competitors,
        critical_deal_flag__c                       AS critical_deal_flag,
        {{sfdc_deal_size('incremental_acv_2__c', 'deal_size')}},
        forecastcategoryname                        AS forecast_category_name,
        incremental_acv_2__c                        AS forecasted_iacv,
        iacv_created_date__c                        AS iacv_created_date,
        incremental_acv__c                          AS incremental_acv,
        pre_covid_iacv__c                           AS pre_covid_iacv,
        invoice_number__c                           AS invoice_number,
        is_refund_opportunity__c                    AS is_refund,
        is_downgrade_opportunity__c                 AS is_downgrade,
        swing_deal__c                               AS is_swing_deal,
        is_edu_oss_opportunity__c                   AS is_edu_oss,
        is_ps_opportunity__c                        AS is_ps_opp,
        net_iacv__c                                 AS net_incremental_acv,
        campaignid                                  AS primary_campaign_source_id,
        probability                                 AS probability,
        professional_services_value__c              AS professional_services_value,
        push_counter__c                             AS pushed_count,
        reason_for_lost__c                          AS reason_for_loss,
        reason_for_lost_details__c                  AS reason_for_loss_details,
        refund_iacv__c                              AS refund_iacv,
        downgrade_iacv__c                           AS downgrade_iacv,
        renewal_acv__c                              AS renewal_acv,
        renewal_amount__c                           AS renewal_amount,
        {{ sales_qualified_source_cleaning('sql_source__c') }}
                                                    AS sales_qualified_source,
        CASE
          WHEN sales_qualified_source = 'BDR Generated' THEN 'SDR Generated'
          WHEN sales_qualified_source LIKE ANY ('Web%', 'Missing%', 'Other') OR sales_qualified_source IS NULL THEN 'Web Direct Generated'
          ELSE sales_qualified_source
        END                                         AS sales_qualified_source_grouped,
        sdr_pipeline_contribution__c                AS sdr_pipeline_contribution,
        solutions_to_be_replaced__c                 AS solutions_to_be_replaced,
        x3_technical_evaluation_date__c
                                                    AS technical_evaluation_date,
        amount                                      AS total_contract_value,
        recurring_amount__c                         AS recurring_amount,
        true_up_amount__c                           AS true_up_amount,
        proserv_amount__c                           AS proserv_amount,
        other_non_recurring_amount__c               AS other_non_recurring_amount,
        upside_iacv__c                              AS upside_iacv,
        upside_swing_deal_iacv__c                   AS upside_swing_deal_iacv,
        web_portal_purchase__c                      AS is_web_portal_purchase,
        opportunity_term__c                         AS opportunity_term,
        pio__c                                      AS partner_initiated_opportunity,
        user_segment_o__c                           AS user_segment,
        start_date__c::DATE                         AS subscription_start_date,
        end_date__c::DATE                           AS subscription_end_date,
        true_up_value__c                            AS true_up_value,
        order_type_live__c                          AS order_type_live,
        order_type_test__c                          AS order_type_stamped,
        CASE 
          WHEN order_type_stamped = '1. New - First Order'
            THEN '1) New - First Order'
          WHEN order_type_stamped IN ('2. New - Connected', '3. Growth', '4. Contraction', '5. Churn - Partial', '6. Churn - Final')
            THEN '2) Growth (Growth / New - Connected / Churn / Contraction)'
          WHEN order_type_stamped IN ('7. PS / Other')
            THEN '3) Consumption / PS / Other'
          ELSE 'Missing order_type_name_grouped'
        END                                         AS order_type_grouped,
        {{ growth_type('order_type_test__c', 'arr_basis__c') }}
                                                    AS growth_type,
        arr_net__c                                  AS net_arr,
        arr_basis__c                                AS arr_basis,
        arr__c                                      AS arr,
        days_in_sao__c                              AS days_in_sao,
        {{ sales_hierarchy_sales_segment_cleaning('user_segment_o__c') }}
                                                    AS user_segment_stamped,
        CASE 
          WHEN user_segment_stamped IN ('Large', 'PubSec') THEN 'Large'
          ELSE user_segment_stamped
        END                                         AS user_segment_stamped_grouped,                                            
        stamped_user_geo__c                         AS user_geo_stamped,
        stamped_user_region__c                      AS user_region_stamped,
        stamped_user_area__c                        AS user_area_stamped,
        {{ sales_segment_region_grouped('user_segment_stamped', 'user_region_stamped') }}
                                                    AS user_segment_region_stamped_grouped,
        stamped_opportunity_owner__c                AS crm_opp_owner_stamped_name,
        stamped_account_owner__c                    AS crm_account_owner_stamped_name,
        sao_opportunity_owner__c                    AS sao_crm_opp_owner_stamped_name,
        sao_account_owner__c                        AS sao_crm_account_owner_stamped_name,
        sao_user_segment__c                         AS sao_crm_opp_owner_sales_segment_stamped,
        sao_user_geo__c                             AS sao_crm_opp_owner_geo_stamped,
        sao_user_region__c                          AS sao_crm_opp_owner_region_stamped,
        sao_user_area__c                            AS sao_crm_opp_owner_area_stamped,
        opportunity_category__c                     AS opportunity_category,
        opportunity_health__c                       AS opportunity_health,
        risk_type__c                                AS risk_type,
        risk_reasons__c                             AS risk_reasons,
        tam_notes__c                                AS tam_notes,
        solution_architect__c                       AS primary_solution_architect,
        product_details__c                          AS product_details,
        product_category__c                         AS product_category,
        products_purchased__c                       AS products_purchased,
        CASE
          WHEN web_portal_purchase__c THEN 'Web Direct'
          WHEN arr_net__c < 5000 THEN '<5K'
          WHEN arr_net__c < 25000 THEN '5-25K'
          WHEN arr_net__c < 100000 THEN '25-100K'
          WHEN arr_net__c < 250000 THEN '100-250K'
          WHEN arr_net__c > 250000 THEN '250K+'
          ELSE 'Missing opportunity_deal_size'
        END opportunity_deal_size,

      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
        sales_segmentation_o__c                     AS segment,
        COALESCE({{ sales_segment_cleaning('sales_segmentation_employees_o__c') }}, {{ sales_segment_cleaning('sales_segmentation_o__c') }}, 'Unknown' )
                                                    AS sales_segment,
        {{ sales_segment_cleaning('ultimate_parent_sales_segment_emp_o__c') }}
                                                    AS parent_segment,
      -- ************************************

        -- dates in stage fields
        days_in_0_pending_acceptance__c             AS days_in_0_pending_acceptance,
        days_in_1_discovery__c                      AS days_in_1_discovery,
        days_in_2_scoping__c                        AS days_in_2_scoping,
        days_in_3_technical_evaluation__c           AS days_in_3_technical_evaluation,
        days_in_4_proposal__c                       AS days_in_4_proposal,
        days_in_5_negotiating__c                    AS days_in_5_negotiating,

        x0_pending_acceptance_date__c               AS stage_0_pending_acceptance_date,
        x1_discovery_date__c                        AS stage_1_discovery_date,
        x2_scoping_date__c                          AS stage_2_scoping_date,
        x3_technical_evaluation_date__c             AS stage_3_technical_evaluation_date,
        x4_proposal_date__c                         AS stage_4_proposal_date,
        x5_negotiating_date__c                      AS stage_5_negotiating_date,
        x6_awaiting_signature_date__c               AS stage_6_awaiting_signature_date,
        x6_closed_won_date__c                       AS stage_6_closed_won_date,
        x7_closed_lost_date__c                      AS stage_6_closed_lost_date,

        -- sales segment fields
        COALESCE({{ sales_segment_cleaning('sales_segmentation_employees_o__c') }}, {{ sales_segment_cleaning('sales_segmentation_o__c') }}, 'Unknown' )
                                                   AS division_sales_segment_stamped,
        -- channel reporting
        -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
        dr_partner_deal_type__c                     AS dr_partner_deal_type,
        dr_partner_engagement__c                    AS dr_partner_engagement,
        {{ channel_type('dr_partner_engagement', 'order_type_stamped') }}
                                                    AS channel_type,
        impartnerprm__partneraccount__c             AS partner_account,
        vartopiadrs__dr_status1__c                  AS dr_status,
        distributor__c                              AS distributor,
        influence_partner__c                        AS influence_partner,
        fulfillment_partner__c                      AS fulfillment_partner,
        platform_partner__c                         AS platform_partner,
        partner_track__c                            AS partner_track,
        public_sector_opp__c::BOOLEAN               AS is_public_sector_opp,
        registration_from_portal__c::BOOLEAN        AS is_registration_from_portal,
        calculated_discount__c                      AS calculated_discount,
        partner_discount__c                         AS partner_discount,
        partner_discount_calc__c                    AS partner_discount_calc,
        comp_channel_neutral__c                     AS comp_channel_neutral,

        -- command plan fields
        fm_champion__c                              AS cp_champion,
        fm_close_plan__c                            AS cp_close_plan,
        fm_competition__c                           AS cp_competition,
        fm_decision_criteria__c                     AS cp_decision_criteria,
        fm_decision_process__c                      AS cp_decision_process,
        fm_economic_buyer__c                        AS cp_economic_buyer,
        fm_identify_pain__c                         AS cp_identify_pain,
        fm_metrics__c                               AS cp_metrics,
        fm_risks__c                                 AS cp_risks,
        fm_use_cases__c                             AS cp_use_cases,
        fm_value_driver__c                          AS cp_value_driver,
        fm_why_do_anything_at_all__c                AS cp_why_do_anything_at_all,
        fm_why_gitlab__c                            AS cp_why_gitlab,
        fm_why_now__c                               AS cp_why_now,

        -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6577
        sa_validated_tech_evaluation_close_statu__c AS sa_tech_evaluation_close_status,
        sa_validated_tech_evaluation_end_date__c    AS sa_tech_evaluation_end_date,
        sa_validated_tech_evaluation_start_date__c  AS sa_tech_evaluation_start_date,

        -- metadata
        convert_timezone('America/Los_Angeles',convert_timezone('UTC',
                 CURRENT_TIMESTAMP()))              AS _last_dbt_run,
        DATEDIFF(days, lastactivitydate::date,
                         CURRENT_DATE)              AS days_since_last_activity,
        isdeleted                                   AS is_deleted,
        lastactivitydate                            AS last_activity_date,
        recordtypeid                                AS record_type_id

      FROM source
  )

SELECT *
FROM renamed
