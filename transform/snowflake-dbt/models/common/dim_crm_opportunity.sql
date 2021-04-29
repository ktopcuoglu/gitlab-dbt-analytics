WITH sfdc_opportunity AS (

    SELECT *
    FROM {{ref('sfdc_opportunity')}}

), sfdc_opportunity_stage AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_stage')}}

), layered AS (

    SELECT
      -- keys
      sfdc_opportunity.account_id                       AS dim_crm_account_id,
      sfdc_opportunity.opportunity_id                   AS dim_crm_opportunity_id,
      sfdc_opportunity.opportunity_name,
      sfdc_opportunity.owner_id                         AS dim_crm_sales_rep_id,

      -- logistical information
      sfdc_opportunity.generated_source,
      sfdc_opportunity.lead_source,
      sfdc_opportunity.merged_opportunity_id,
      sfdc_opportunity.net_new_source_categories,
      sfdc_opportunity.account_owner_team_stamped,
      sfdc_opportunity.primary_campaign_source_id       AS primary_campaign_source_id,
      sfdc_opportunity.sales_accepted_date,
      sfdc_opportunity.sales_path,
      sfdc_opportunity.sales_type,
      sfdc_opportunity.source_buckets,
      sfdc_opportunity.opportunity_sales_development_representative,
      sfdc_opportunity.opportunity_business_development_representative,
      sfdc_opportunity.opportunity_development_representative,
      sfdc_opportunity.stage_name,
      sfdc_opportunity_stage.is_active                  AS stage_is_active,
      sfdc_opportunity_stage.is_closed                  AS stage_is_closed,
      sfdc_opportunity.technical_evaluation_date,
      sfdc_opportunity.deal_path,

      -- opportunity information
      
      sfdc_opportunity.product_category,
      sfdc_opportunity.product_details,
      sfdc_opportunity.products_purchased,
      sfdc_opportunity.competitors,
      sfdc_opportunity.critical_deal_flag,
      sfdc_opportunity.forecast_category_name,
      sfdc_opportunity.invoice_number,
      sfdc_opportunity.is_refund,
      sfdc_opportunity.is_downgrade,
      CASE
        WHEN (sfdc_opportunity.days_in_stage > 30
          OR sfdc_opportunity.incremental_acv > 100000
          OR sfdc_opportunity.pushed_count > 0)
          THEN TRUE
          ELSE FALSE
      END                                               AS is_risky,
      sfdc_opportunity.is_swing_deal,
      sfdc_opportunity.is_edu_oss,
      sfdc_opportunity_stage.is_won                     AS is_won,
      sfdc_opportunity.is_ps_opp,
      sfdc_opportunity.probability,
      sfdc_opportunity.professional_services_value,
      sfdc_opportunity.reason_for_loss,
      sfdc_opportunity.reason_for_loss_details,
      sfdc_opportunity.sales_qualified_source,
      sfdc_opportunity.sales_qualified_source_grouped,
      sfdc_opportunity.solutions_to_be_replaced,
      sfdc_opportunity.is_web_portal_purchase,
      sfdc_opportunity.partner_initiated_opportunity,
      sfdc_opportunity.user_segment,
      sfdc_opportunity.order_type_stamped               AS order_type,
      sfdc_opportunity.opportunity_category,
      sfdc_opportunity.opportunity_health,
      sfdc_opportunity.risk_type,
      sfdc_opportunity.risk_reasons,
      sfdc_opportunity.tam_notes,

      -- stamped fields
      sfdc_opportunity.crm_opp_owner_stamped_name,
      sfdc_opportunity.crm_account_owner_stamped_name,
      sfdc_opportunity.sao_crm_opp_owner_stamped_name,
      sfdc_opportunity.sao_crm_account_owner_stamped_name,
      sfdc_opportunity.sao_crm_opp_owner_sales_segment_stamped,
      sfdc_opportunity.sao_crm_opp_owner_geo_stamped,
      sfdc_opportunity.sao_crm_opp_owner_region_stamped,
      sfdc_opportunity.sao_crm_opp_owner_area_stamped,

      -- ************************************
      -- channel reporting
      -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
      sfdc_opportunity.dr_partner_deal_type,
      sfdc_opportunity.dr_partner_engagement,

      -- metadata
      sfdc_opportunity._last_dbt_run

    FROM sfdc_opportunity
    INNER JOIN sfdc_opportunity_stage
      ON sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label

)

{{ dbt_audit(
    cte_ref="layered",
    created_by="@iweeks",
    updated_by="@jpeguero",
    created_date="2020-11-20",
    updated_date="2021-04-29"
) }}
