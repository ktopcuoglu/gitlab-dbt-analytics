{{ simple_cte([
    ('fct_crm_opportunity','fct_crm_opportunity_daily_snapshot'),
    ('dim_crm_account','dim_crm_account_daily_snapshot')
]) }}

, final AS (
 

    SELECT
      fct_crm_opportunity.snapshot_id,
      fct_crm_opportunity.dim_crm_opportunity_id,
      fct_crm_opportunity.opportunity_name,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.dim_crm_account_id,
      fct_crm_opportunity.dim_crm_user_id,

      -- dates
      fct_crm_opportunity.sales_accepted_date,
      DATE_TRUNC('month', fct_crm_opportunity.sales_accepted_date)           AS sales_accepted_month,
      fct_crm_opportunity.close_date,
      DATE_TRUNC('month', fct_crm_opportunity.close_date)                    AS close_month,
      fct_crm_opportunity.created_date,
      DATE_TRUNC('month', fct_crm_opportunity.created_date)                  AS created_month,

      -- opportunity attributes & additive fields
      fct_crm_opportunity.is_won,
      fct_crm_opportunity.is_closed,
      fct_crm_opportunity.days_in_sao,
      fct_crm_opportunity.arr_basis,
      fct_crm_opportunity.incremental_acv                                  AS iacv,
      fct_crm_opportunity.net_incremental_acv                              AS net_iacv,
      fct_crm_opportunity.net_arr,
      fct_crm_opportunity.new_logo_count,
      fct_crm_opportunity.amount,
      fct_crm_opportunity.is_edu_oss,
      fct_crm_opportunity.is_ps_opp,
      fct_crm_opportunity.stage_name,
      fct_crm_opportunity.reason_for_loss,
      fct_crm_opportunity.sales_type,
      fct_crm_opportunity.is_sao,
      fct_crm_opportunity.is_net_arr_closed_deal,
      fct_crm_opportunity.is_new_logo_first_order,
      fct_crm_opportunity.is_net_arr_pipeline_created,
      fct_crm_opportunity.is_win_rate_calc,
      fct_crm_opportunity.is_closed_won,
      fct_crm_opportunity.deal_path                                     AS deal_path_name,
      fct_crm_opportunity.order_type,
      fct_crm_opportunity.order_type_grouped,
      fct_crm_opportunity.dr_partner_engagement                         AS dr_partner_engagement_name,
      fct_crm_opportunity.alliance_type                                 AS alliance_type_name,
      fct_crm_opportunity.alliance_type_short                           AS alliance_type_short_name,
      fct_crm_opportunity.channel_type                                  AS channel_type_name,
      fct_crm_opportunity.sales_qualified_source                        AS sales_qualified_source_name,
      fct_crm_opportunity.sales_qualified_source_grouped,
      fct_crm_opportunity.sqs_bucket_engagement,
      fct_crm_opportunity.closed_buckets,
      fct_crm_opportunity.duplicate_opportunity_id,
      fct_crm_opportunity.opportunity_category,
      fct_crm_opportunity.source_buckets,
      fct_crm_opportunity.opportunity_sales_development_representative,
      fct_crm_opportunity.opportunity_business_development_representative,
      fct_crm_opportunity.opportunity_development_representative,
      fct_crm_opportunity.sdr_or_bdr,
      fct_crm_opportunity.iqm_submitted_by_role,
      fct_crm_opportunity.sdr_pipeline_contribution,
      fct_crm_opportunity.is_web_portal_purchase,
      fct_crm_opportunity.fpa_master_bookings_flag,
      fct_crm_opportunity.sales_path,
      fct_crm_opportunity.professional_services_value,
      fct_crm_opportunity.primary_solution_architect,
      fct_crm_opportunity.product_details,
      fct_crm_opportunity.product_category,
      fct_crm_opportunity.products_purchased,
      fct_crm_opportunity.growth_type,
      fct_crm_opportunity.opportunity_deal_size,
      dim_crm_account.is_jihu_account,
      dim_crm_account.fy22_new_logo_target_list,
      dim_crm_account.crm_account_gtm_strategy,
      dim_crm_account.crm_account_focus_account,
      dim_crm_account.crm_account_zi_technologies,
      dim_crm_account.parent_crm_account_gtm_strategy,
      dim_crm_account.parent_crm_account_focus_account,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_zi_technologies,
      dim_crm_account.parent_crm_account_demographics_sales_segment,
      dim_crm_account.parent_crm_account_demographics_geo,
      dim_crm_account.parent_crm_account_demographics_region,
      dim_crm_account.parent_crm_account_demographics_area,
      dim_crm_account.parent_crm_account_demographics_territory,
      dim_crm_account.crm_account_demographics_employee_count,
      dim_crm_account.parent_crm_account_demographics_max_family_employee,
      dim_crm_account.parent_crm_account_demographics_upa_country,
      dim_crm_account.parent_crm_account_demographics_upa_state,
      dim_crm_account.parent_crm_account_demographics_upa_city,
      dim_crm_account.parent_crm_account_demographics_upa_street,
      dim_crm_account.parent_crm_account_demographics_upa_postal_code,

      -- crm opp owner/account owner fields stamped at SAO date
      fct_crm_opportunity.sao_crm_opp_owner_stamped_name,
      fct_crm_opportunity.sao_crm_account_owner_stamped_name,
      fct_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped,
      fct_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped_grouped,
      fct_crm_opportunity.sao_crm_opp_owner_geo_stamped,
      fct_crm_opportunity.sao_crm_opp_owner_region_stamped,
      fct_crm_opportunity.sao_crm_opp_owner_area_stamped,
      fct_crm_opportunity.sao_crm_opp_owner_segment_region_stamped_grouped,
      fct_crm_opportunity.sao_crm_opp_owner_sales_segment_geo_region_area_stamped,

      -- crm opp owner/account owner stamped fields stamped at close date
      fct_crm_opportunity.crm_opp_owner_stamped_name,
      fct_crm_opportunity.crm_account_owner_stamped_name,
      fct_crm_opportunity.user_segment_stamped                            AS crm_opp_owner_sales_segment_stamped,
      fct_crm_opportunity.user_segment_stamped_grouped                    AS crm_opp_owner_sales_segment_stamped_grouped,
      fct_crm_opportunity.user_geo_stamped                                AS crm_opp_owner_geo_stamped,
      fct_crm_opportunity.user_region_stamped                             AS crm_opp_owner_region_stamped,
      fct_crm_opportunity.user_area_stamped                               AS crm_opp_owner_area_stamped,
      {{ sales_segment_region_grouped('fct_crm_opportunity.user_segment_stamped',
        'fct_crm_opportunity.user_geo_stamped', 'fct_crm_opportunity.user_region_stamped') }}
                                                                          AS crm_opp_owner_sales_segment_region_stamped_grouped,
      fct_crm_opportunity.crm_opp_owner_sales_segment_geo_region_area_stamped,
      fct_crm_opportunity.crm_opp_owner_user_role_type_stamped,

      --crm owner/sales rep live fields
      fct_crm_opportunity.user_segment                                    AS crm_user_sales_segment,
      --fct_crm_opportunity.user_sales_segment_grouped                                        AS crm_user_sales_segment_grouped,
      fct_crm_opportunity.user_geo                                        AS crm_user_geo,
      fct_crm_opportunity.user_region                                     AS crm_user_region,
      fct_crm_opportunity.user_area                                       AS crm_user_area,
      {{ sales_segment_region_grouped('fct_crm_opportunity.crm_user_sales_segment',
        'fct_crm_opportunity.crm_user_geo', 'fct_crm_opportunity.crm_user_region') }}
                                                                           AS crm_user_sales_segment_region_grouped,

      --  -- crm account owner/sales rep live fields
      -- fct_crm_opportunity.crm_user_sales_segment                            AS crm_account_user_sales_segment,
      -- fct_crm_opportunity.crm_user_sales_segment_grouped   AS crm_account_user_sales_segment_grouped,
      -- fct_crm_opportunity.crm_user_geo                               AS crm_account_user_geo,
      -- fct_crm_opportunity.crm_user_region                         AS crm_account_user_region,
      -- fct_crm_opportunity.crm_user_area                             AS crm_account_user_area,

      -- channel fields
      fct_crm_opportunity.lead_source,
      fct_crm_opportunity.dr_partner_deal_type,
      fct_crm_opportunity.partner_account,
      fct_crm_opportunity.dr_status,
      fct_crm_opportunity.distributor,
      fct_crm_opportunity.dr_deal_id,
      fct_crm_opportunity.dr_primary_registration,
      fct_crm_opportunity.influence_partner,
      fct_crm_opportunity.fulfillment_partner,
      fct_crm_opportunity.platform_partner,
      fct_crm_opportunity.partner_track,
      fct_crm_opportunity.is_public_sector_opp,
      fct_crm_opportunity.is_registration_from_portal,
      fct_crm_opportunity.calculated_discount,
      fct_crm_opportunity.partner_discount,
      fct_crm_opportunity.partner_discount_calc,
      fct_crm_opportunity.comp_channel_neutral,
      fct_crm_opportunity.count_crm_attribution_touchpoints,
      fct_crm_opportunity.weighted_linear_iacv,
      fct_crm_opportunity.count_campaigns,

      -- Solutions-Architech fields
      fct_crm_opportunity.sa_tech_evaluation_close_status,
      fct_crm_opportunity.sa_tech_evaluation_end_date,
      fct_crm_opportunity.sa_tech_evaluation_start_date,


      -- Command Plan fields
      fct_crm_opportunity.cp_partner,
      fct_crm_opportunity.cp_paper_process,
      fct_crm_opportunity.cp_help,
      fct_crm_opportunity.cp_review_notes

    FROM fct_crm_opportunity
    LEFT JOIN dim_crm_account
      ON fct_crm_opportunity.dim_crm_account_id = dim_crm_account.dim_crm_account_id
        AND fct_crm_opportunity.snapshot_id = dim_crm_account.snapshot_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-04-15",
    updated_date="2022-04-15"
  ) }}