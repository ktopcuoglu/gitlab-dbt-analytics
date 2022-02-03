WITH source AS (

    SELECT *
    FROM {{ ref('prep_crm_opportunity') }}

), result AS (

    SELECT
      -- keys
      dim_crm_account_id,
      dim_crm_opportunity_id,
      dim_crm_user_id,

      -- logistical information
      generated_source,
      lead_source,
      merged_opportunity_id,
      duplicate_opportunity_id,
      net_new_source_categories,
      account_owner_team_stamped,
      primary_campaign_source_id,
      sales_accepted_date,
      sales_path,
      sales_type,
      source_buckets,
      opportunity_sales_development_representative,
      opportunity_business_development_representative,
      opportunity_development_representative,
      iqm_submitted_by_role,
      sdr_pipeline_contribution,
      stage_name,
      stage_is_active,
      stage_is_closed,
      technical_evaluation_date,
      sa_tech_evaluation_close_status,
      sa_tech_evaluation_end_date,
      sa_tech_evaluation_start_date,
      deal_path,

      -- opportunity information
      opportunity_name,
      product_category,
      product_details,
      products_purchased,
      competitors,
      critical_deal_flag,
      forecast_category_name,
      invoice_number,
      is_refund,
      is_downgrade,
      is_risky,
      is_swing_deal,
      is_edu_oss,
      is_won,
      is_ps_opp,
      probability,
      professional_services_value,
      reason_for_loss,
      reason_for_loss_details,
      sales_qualified_source,
      sales_qualified_source_grouped,
      solutions_to_be_replaced,
      is_web_portal_purchase,
      partner_initiated_opportunity,
      user_segment,
      order_type,
      opportunity_category,
      opportunity_health,
      risk_type,
      risk_reasons,
      tam_notes,
      payment_schedule,
      comp_y2_iacv,
      opportunity_term,
      subscription_start_date,
      subscription_end_date,

      -- quote fields
      quote_start_date,

      -- Command Plan fields
      cp_partner,
      cp_paper_process,
      cp_help,
      cp_review_notes,

      -- stamped fields
      crm_opp_owner_stamped_name,
      crm_account_owner_stamped_name,
      sao_crm_opp_owner_stamped_name,
      sao_crm_account_owner_stamped_name,
      sao_crm_opp_owner_sales_segment_stamped,
      sao_crm_opp_owner_geo_stamped,
      sao_crm_opp_owner_region_stamped,
      sao_crm_opp_owner_area_stamped,

      -- channel reporting
      dr_partner_deal_type,
      dr_partner_engagement,

      -- metadata
      _last_dbt_run

    FROM source

)

{{ dbt_audit(
    cte_ref="result",
    created_by="@iweeks",
    updated_by="@michellecooper",
    created_date="2020-11-20",
    updated_date="2022-02-01"
) }}
