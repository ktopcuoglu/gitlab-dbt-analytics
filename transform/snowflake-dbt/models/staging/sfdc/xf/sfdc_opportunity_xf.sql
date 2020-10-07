WITH sfdc_opportunity AS (

    SELECT * FROM {{ref('sfdc_opportunity')}}

), sfdc_opportunity_stage AS (

    SELECT * FROM {{ref('sfdc_opportunity_stage')}}

), sfdc_lead_source AS (

    SELECT * FROM {{ref('sfdc_lead_sources')}}

), sfdc_users_xf AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}
), sfdc_account AS (

    SELECT * FROM {{ref('sfdc_account')}}

), layered AS (

    SELECT
      -- keys
      sfdc_opportunity.account_id,
      sfdc_opportunity.opportunity_id,
      sfdc_opportunity.opportunity_name,
      sfdc_opportunity.owner_id,

      -- logistical information
      sfdc_opportunity.business_type,
      sfdc_opportunity.close_date,
      sfdc_opportunity.created_date,
      sfdc_opportunity.days_in_stage,
      sfdc_opportunity.deployment_preference,
      sfdc_opportunity.generated_source,
      sfdc_opportunity.lead_source,
      sfdc_lead_source.lead_source_id                                                             AS lead_source_id,
      COALESCE(sfdc_lead_source.initial_source, 'Unknown')                                        AS lead_source_name,
      COALESCE(sfdc_lead_source.initial_source_type, 'Unknown')                                   AS lead_source_type,
      sfdc_opportunity.merged_opportunity_id,
      sfdc_opportunity.net_new_source_categories,
      sfdc_opportunity.opportunity_business_development_representative,
      sfdc_opportunity.opportunity_owner                                                          AS opportunity_owner,
      sfdc_opportunity.opportunity_owner_department                                               AS opportunity_owner_department,
      sfdc_opportunity.opportunity_owner_manager                                                  AS opportunity_owner_manager,
      sfdc_users_xf.role_name                                                                     AS opportunity_owner_role,
      sfdc_opportunity.opportunity_owner_team                                                     AS opportunity_owner_team,
      sfdc_users_xf.title                                                                         AS opportunity_owner_title,
      sfdc_opportunity.opportunity_sales_development_representative,
      sfdc_opportunity.opportunity_development_representative,
      sfdc_opportunity.account_owner_team_stamped,
      sfdc_opportunity.opportunity_term,
      sfdc_opportunity.primary_campaign_source_id                                                 AS primary_campaign_source_id,
      sfdc_opportunity.sales_accepted_date,
      sfdc_opportunity.sales_path,
      sfdc_opportunity.sales_qualified_date,
      sfdc_opportunity.sales_type,
      sfdc_opportunity.sdr_pipeline_contribution,
      sfdc_opportunity.source_buckets,
      sfdc_opportunity.stage_name,
      sfdc_opportunity_stage.is_active                                                             AS stage_is_active,
      sfdc_opportunity_stage.is_closed                                                             AS stage_is_closed,
      sfdc_opportunity.technical_evaluation_date,
      sfdc_opportunity.order_type,

      -- opportunity information
      sfdc_opportunity.acv,
      sfdc_opportunity.amount,
      sfdc_opportunity.closed_deals,
      sfdc_opportunity.competitors,
      sfdc_opportunity.critical_deal_flag,
      sfdc_opportunity.deal_size,
      sfdc_opportunity.forecast_category_name,
      sfdc_opportunity.forecasted_iacv,
      sfdc_opportunity.iacv_created_date,
      sfdc_opportunity.incremental_acv,
      sfdc_opportunity.pre_covid_iacv,
      sfdc_opportunity.invoice_number,
      sfdc_opportunity.is_refund,
      sfdc_opportunity.is_downgrade,
      CASE WHEN (sfdc_opportunity.days_in_stage > 30
        OR sfdc_opportunity.incremental_acv > 100000
        OR sfdc_opportunity.pushed_count > 0)
      THEN TRUE
      ELSE FALSE
      END                                                                                         AS is_risky,
      sfdc_opportunity.is_swing_deal,
      sfdc_opportunity.is_edu_oss,
      sfdc_opportunity_stage.is_won                                                               AS is_won,
      sfdc_opportunity.net_incremental_acv,
      sfdc_opportunity.nrv,
      sfdc_opportunity.probability,
      sfdc_opportunity.professional_services_value,
      sfdc_opportunity.pushed_count,
      sfdc_opportunity.reason_for_loss,
      sfdc_opportunity.reason_for_loss_details,
      sfdc_opportunity.refund_iacv,
      sfdc_opportunity.downgrade_iacv,
      sfdc_opportunity.renewal_acv,
      sfdc_opportunity.renewal_amount,
      sfdc_opportunity.sales_qualified_source,
      sfdc_opportunity.solutions_to_be_replaced,
      sfdc_opportunity.total_contract_value,
      sfdc_opportunity.upside_iacv,
      sfdc_opportunity.upside_swing_deal_iacv,
      sfdc_opportunity.incremental_acv * (probability /100)         AS weighted_iacv,
      sfdc_opportunity.is_web_portal_purchase,
      sfdc_opportunity.partner_initiated_opportunity,
      sfdc_opportunity.user_segment,
      sfdc_opportunity.subscription_start_date,
      sfdc_opportunity.subscription_end_date,
      sfdc_opportunity.true_up_value,
      sfdc_opportunity.order_type_live,
      sfdc_opportunity.order_type_stamped,
      sfdc_opportunity.net_arr,

      -- days and dates per stage
      sfdc_opportunity.days_in_1_discovery,
      sfdc_opportunity.days_in_2_scoping,
      sfdc_opportunity.days_in_3_technical_evaluation,
      sfdc_opportunity.days_in_4_proposal,
      sfdc_opportunity.days_in_5_negotiating,
      sfdc_opportunity.stage_0_pending_acceptance_date,
      sfdc_opportunity.stage_1_discovery_date,
      sfdc_opportunity.stage_2_scoping_date,
      sfdc_opportunity.stage_3_technical_evaluation_date,
      sfdc_opportunity.stage_4_proposal_date,
      sfdc_opportunity.stage_5_negotiating_date,
      sfdc_opportunity.stage_6_awaiting_signature_date,
      sfdc_opportunity.stage_6_closed_won_date,
      sfdc_opportunity.stage_6_closed_lost_date,

      -- command plan fields
      sfdc_opportunity.cp_champion,
      sfdc_opportunity.cp_close_plan,
      sfdc_opportunity.cp_competition,
      sfdc_opportunity.cp_decision_criteria,
      sfdc_opportunity.cp_decision_process,
      sfdc_opportunity.cp_economic_buyer,
      sfdc_opportunity.cp_identify_pain,
      sfdc_opportunity.cp_metrics,
      sfdc_opportunity.cp_risks,
      sfdc_opportunity.cp_use_cases,
      sfdc_opportunity.cp_value_driver,
      sfdc_opportunity.cp_why_do_anything_at_all,
      sfdc_opportunity.cp_why_gitlab,
      sfdc_opportunity.cp_why_now,

      -- sales segment refactor
      sfdc_opportunity.division_sales_segment_stamped,
      sfdc_account.tsp_max_hierarchy_sales_segment,
      sfdc_account.division_sales_segment,
      sfdc_account.ultimate_parent_sales_segment,

      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
      -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/5709
      sfdc_opportunity.segment                          AS segment,
      sfdc_opportunity.sales_segment                    AS sales_segment,
      sfdc_opportunity.parent_segment                   AS parent_segment,
      
      -- ************************************
      -- channel reporting
      -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
      sfdc_opportunity.dr_partner_deal_type,
      sfdc_opportunity.dr_partner_engagement,

      -- metadata
      sfdc_opportunity._last_dbt_run,
      sfdc_record_type.business_process_id,
      sfdc_opportunity.days_since_last_activity,
      sfdc_opportunity.is_deleted,
      sfdc_opportunity.last_activity_date,
      sfdc_record_type.record_type_description,
      sfdc_opportunity.record_type_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_modifying_object_type,
      sfdc_record_type.record_type_name,
      md5((date_trunc('month', sfdc_opportunity.close_date)::date)||UPPER(sfdc_users_xf.team))    AS region_quota_id,
      md5((date_trunc('month', sfdc_opportunity.close_date)::date)||UPPER(sfdc_users_xf.name))    AS sales_quota_id

  FROM sfdc_opportunity
  INNER JOIN sfdc_opportunity_stage
    ON sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
  LEFT JOIN sfdc_lead_source
    ON sfdc_opportunity.lead_source = sfdc_lead_source.initial_source
  LEFT JOIN sfdc_users_xf
    ON sfdc_opportunity.owner_id = sfdc_users_xf.user_id
  LEFT JOIN sfdc_record_type
    ON sfdc_opportunity.record_type_id = sfdc_record_type.record_type_id
  LEFT JOIN sfdc_account
    ON sfdc_account.account_id = sfdc_opportunity.account_id

)

SELECT *
FROM layered
