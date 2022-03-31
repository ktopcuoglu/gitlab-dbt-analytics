WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}

), layered AS (

    SELECT
      -- keys
      prep_crm_opportunity.dim_crm_account_id,
      prep_crm_opportunity.dim_crm_opportunity_id,
      prep_crm_opportunity.opportunity_name,
      prep_crm_opportunity.dim_crm_user_id,

      -- logistical information
      prep_crm_opportunity.generated_source,
      prep_crm_opportunity.lead_source,
      prep_crm_opportunity.merged_opportunity_id,
      prep_crm_opportunity.duplicate_opportunity_id,
      prep_crm_opportunity.net_new_source_categories,
      prep_crm_opportunity.account_owner_team_stamped,
      prep_crm_opportunity.primary_campaign_source_id,
      prep_crm_opportunity.sales_accepted_date,
      prep_crm_opportunity.sales_path,
      prep_crm_opportunity.sales_type,
      prep_crm_opportunity.source_buckets,
      prep_crm_opportunity.opportunity_sales_development_representative,
      prep_crm_opportunity.opportunity_business_development_representative,
      prep_crm_opportunity.opportunity_development_representative,
      prep_crm_opportunity.sdr_or_bdr,
      prep_crm_opportunity.iqm_submitted_by_role,
      prep_crm_opportunity.sdr_pipeline_contribution,
      prep_crm_opportunity.stage_name,
      prep_crm_opportunity.stage_is_active,
      prep_crm_opportunity.stage_is_closed,
      prep_crm_opportunity.technical_evaluation_date,
      prep_crm_opportunity.sa_tech_evaluation_close_status,
      prep_crm_opportunity.sa_tech_evaluation_end_date,
      prep_crm_opportunity.sa_tech_evaluation_start_date,
      prep_crm_opportunity.deal_path,

      -- opportunity information

      prep_crm_opportunity.product_category,
      prep_crm_opportunity.product_details,
      prep_crm_opportunity.products_purchased,
      prep_crm_opportunity.competitors,
      prep_crm_opportunity.critical_deal_flag,
      prep_crm_opportunity.forecast_category_name,
      prep_crm_opportunity.invoice_number,
      prep_crm_opportunity.is_refund,
      prep_crm_opportunity.is_downgrade,
      prep_crm_opportunity.is_risky,
      prep_crm_opportunity.is_swing_deal,
      prep_crm_opportunity.is_edu_oss,
      prep_crm_opportunity.is_won,
      prep_crm_opportunity.is_ps_opp,
      prep_crm_opportunity.probability,
      prep_crm_opportunity.professional_services_value,
      prep_crm_opportunity.reason_for_loss,
      prep_crm_opportunity.reason_for_loss_details,
      prep_crm_opportunity.sales_qualified_source,
      prep_crm_opportunity.sales_qualified_source_grouped,
      prep_crm_opportunity.solutions_to_be_replaced,
      prep_crm_opportunity.is_web_portal_purchase,
      prep_crm_opportunity.partner_initiated_opportunity,
      prep_crm_opportunity.user_segment,
      prep_crm_opportunity.order_type,
      prep_crm_opportunity.opportunity_category,
      prep_crm_opportunity.opportunity_health,
      prep_crm_opportunity.risk_type,
      prep_crm_opportunity.risk_reasons,
      prep_crm_opportunity.tam_notes,
      prep_crm_opportunity.payment_schedule,
      prep_crm_opportunity.comp_y2_iacv,
      prep_crm_opportunity.opportunity_term,
      prep_crm_opportunity.quote_start_date,
      prep_crm_opportunity.subscription_start_date,
      prep_crm_opportunity.subscription_end_date,

      -- Command Plan fields
      prep_crm_opportunity.cp_partner,
      prep_crm_opportunity.cp_paper_process,
      prep_crm_opportunity.cp_help,
      prep_crm_opportunity.cp_review_notes,

      -- stamped fields
      prep_crm_opportunity.crm_opp_owner_stamped_name,
      prep_crm_opportunity.crm_account_owner_stamped_name,
      prep_crm_opportunity.sao_crm_opp_owner_stamped_name,
      prep_crm_opportunity.sao_crm_account_owner_stamped_name,
      prep_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped,
      prep_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped_grouped,
      prep_crm_opportunity.sao_crm_opp_owner_geo_stamped,
      prep_crm_opportunity.sao_crm_opp_owner_region_stamped,
      prep_crm_opportunity.sao_crm_opp_owner_area_stamped,
      prep_crm_opportunity.sao_crm_opp_owner_segment_region_stamped_grouped,
      prep_crm_opportunity.sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
      prep_crm_opportunity.crm_opp_owner_user_role_type_stamped,
      prep_crm_opportunity.crm_opp_owner_sales_segment_geo_region_area_stamped,

      -- channel reporting
      prep_crm_opportunity.dr_partner_deal_type,
      prep_crm_opportunity.dr_partner_engagement,

      -- metadata
      prep_crm_opportunity._last_dbt_run

    FROM prep_crm_opportunity

)

{{ dbt_audit(
    cte_ref="layered",
    created_by="@iweeks",
    updated_by="@michellecooper",
    created_date="2020-11-20",
    updated_date="2022-03-07"
) }}
