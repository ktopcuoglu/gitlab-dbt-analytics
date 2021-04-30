
{{ simple_cte([
    ('crm_account_dimensions', 'map_crm_account'),
    ('order_type', 'prep_order_type'),
    ('sales_qualified_source', 'prep_sales_qualified_source'),
    ('deal_path', 'prep_deal_path'),
    ('sales_rep', 'prep_crm_sales_representative'),
    ('sales_segment', 'prep_sales_segment'),
    ('sfdc_campaigns', 'prep_campaign')

]) }}

, first_contact  AS (

    SELECT

      opportunity_id,                                                             -- opportunity_id
      contact_id                                                                  AS sfdc_contact_id,
      md5(cast(coalesce(cast(contact_id as varchar), '') as varchar))             AS dim_crm_person_id,
      ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date ASC)   AS row_num

    FROM {{ ref('sfdc_opportunity_contact_role_source')}}

), user_hierarchy_stamped_sales_segment AS (

    SELECT DISTINCT
      dim_crm_opp_owner_sales_segment_stamped_id,
      crm_opp_owner_sales_segment_stamped
    FROM {{ ref('prep_crm_user_hierarchy_stamped') }}

), user_hierarchy_stamped_geo AS (

    SELECT DISTINCT
      dim_crm_opp_owner_geo_stamped_id,
      crm_opp_owner_geo_stamped
    FROM {{ ref('prep_crm_user_hierarchy_stamped') }}

), user_hierarchy_stamped_region AS (

    SELECT DISTINCT
      dim_crm_opp_owner_region_stamped_id,
      crm_opp_owner_region_stamped
    FROM {{ ref('prep_crm_user_hierarchy_stamped') }}

), user_hierarchy_stamped_area AS (

    SELECT DISTINCT
      dim_crm_opp_owner_area_stamped_id,
      crm_opp_owner_area_stamped
    FROM {{ ref('prep_crm_user_hierarchy_stamped') }}

), sfdc_opportunity AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source')}}
    WHERE is_deleted = 'FALSE'

), attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), opportunity_fields AS(

    SELECT

      opportunity_id                                            AS dim_crm_opportunity_id,
      merged_opportunity_id                                     AS merged_crm_opportunity_id,
      account_id                                                AS dim_crm_account_id,
      owner_id                                                  AS dim_crm_sales_rep_id,
      incremental_acv                                           AS iacv,
      net_arr,
      amount,
      recurring_amount,
      true_up_amount,
      proserv_amount,
      other_non_recurring_amount,
      arr_basis,
      arr,
      subscription_start_date,
      subscription_end_date,
      created_date::DATE                                        AS created_date,
      {{ get_date_id('created_date') }}                         AS created_date_id,
      sales_accepted_date::DATE                                 AS sales_accepted_date,
      {{ get_date_id('sales_accepted_date') }}                  AS sales_accepted_date_id,
      close_date::DATE                                          AS close_date,
      {{ get_date_id('close_date') }}                           AS close_date_id,
      stage_0_pending_acceptance_date::DATE                     AS stage_0_pending_acceptance_date,
      {{ get_date_id('stage_0_pending_acceptance_date') }}      AS stage_0_pending_acceptance_date_id,
      stage_1_discovery_date::DATE                              AS stage_1_discovery_date,
      {{ get_date_id('stage_1_discovery_date') }}               AS stage_1_discovery_date_id,
      stage_2_scoping_date::DATE                                AS stage_2_scoping_date,
      {{ get_date_id('stage_2_scoping_date') }}                 AS stage_2_scoping_date_id,
      stage_3_technical_evaluation_date::DATE                   AS stage_3_technical_evaluation_date,
      {{ get_date_id('stage_3_technical_evaluation_date') }}    AS stage_3_technical_evaluation_date_id,
      stage_4_proposal_date::DATE                               AS stage_4_proposal_date,
      {{ get_date_id('stage_4_proposal_date') }}                AS stage_4_proposal_date_id,
      stage_5_negotiating_date::DATE                            AS stage_5_negotiating_date,
      {{ get_date_id('stage_5_negotiating_date') }}             AS stage_5_negotiating_date_id,
      stage_6_closed_won_date::DATE                             AS stage_6_closed_won_date,
      {{ get_date_id('stage_6_closed_won_date') }}              AS stage_6_closed_won_date_id,
      stage_6_closed_lost_date::DATE                            AS stage_6_closed_lost_date,
      {{ get_date_id('stage_6_closed_lost_date') }}             AS stage_6_closed_lost_date_id,
      days_in_0_pending_acceptance,
      days_in_1_discovery,
      days_in_2_scoping,
      days_in_3_technical_evaluation,
      days_in_4_proposal,
      days_in_5_negotiating,
      is_closed,
      is_won,
      is_refund,
      is_downgrade,
      is_swing_deal,
      is_edu_oss,
      is_web_portal_purchase,
      deal_path,
      order_type_stamped                                        AS order_type,
      sales_segment,
      {{ sales_qualified_source_cleaning('sales_qualified_source') }}
                                                                AS sales_qualified_source,
      days_in_sao,
      user_segment_stamped                                      AS crm_opp_owner_sales_segment_stamped,
      user_geo_stamped                                          AS crm_opp_owner_geo_stamped,
      user_region_stamped                                       AS crm_opp_owner_region_stamped,
      user_area_stamped                                         AS crm_opp_owner_area_stamped,
      primary_solution_architect,
      product_details,
      dr_partner_deal_type,
      dr_partner_engagement,
      partner_account,
      dr_status,
      distributor,
      influence_partner,
      fulfillment_partner,
      platform_partner,
      partner_track,
      is_public_sector_opp,
      is_registration_from_portal,
      calculated_discount,
      partner_discount,
      partner_discount_calc,
      comp_channel_neutral,
      lead_source

    FROM sfdc_opportunity

), linear_attribution_base AS ( --the number of attribution touches a given opp has in total
    --linear attribution IACV of an opp / all touches (count_touches) for each opp - weighted by the number of touches in the given bucket (campaign,channel,etc)
    SELECT
     opportunity_id                                         AS dim_crm_opportunity_id,
     COUNT(DISTINCT attribution_touchpoints.touchpoint_id)  AS count_crm_attribution_touchpoints
    FROM  attribution_touchpoints
    GROUP BY 1

), campaigns_per_opp as (

    SELECT
      opportunity_id                                        AS dim_crm_opportunity_id,
      COUNT(DISTINCT attribution_touchpoints.campaign_id)   AS count_campaigns
    FROM attribution_touchpoints
    GROUP BY 1

), is_sao AS (

    SELECT

      opportunity_id,
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND is_edu_oss = 0
          AND stage_name != '10-Duplicate'
            THEN TRUE
      	ELSE FALSE
      END                                                                         AS is_sao

    FROM sfdc_opportunity

), is_sdr_sao AS (

    SELECT

      opportunity_id,
      CASE
        WHEN opportunity_id in (select opportunity_id from is_sao where is_sao = true)
          AND sales_qualified_source IN (
                                        'SDR Generated'
                                        , 'BDR Generated'
                                        )
            THEN TRUE
        ELSE FALSE
      END                                                                         AS is_sdr_sao

    FROM sfdc_opportunity

), is_net_arr_closed_deal AS (

    SELECT

      opportunity_id,
      CASE
        WHEN (is_won = 'TRUE' OR (sales_type = 'Renewal' AND stage_name = '8-Closed Lost'))
            THEN TRUE
        ELSE FALSE 
      END                                                                         AS is_net_arr_closed_deal

    FROM sfdc_opportunity

), is_new_logo_first_order AS (

    SELECT

      opportunity_id,
      CASE
        WHEN is_won = 'TRUE'
          AND is_closed = 'TRUE'
          AND is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                         AS is_new_logo_first_order

    FROM sfdc_opportunity

), is_net_arr_pipeline_created AS (

    SELECT

      opportunity_id,
      CASE
        WHEN is_edu_oss = 0
          AND stage_name NOT IN (
                                '00-Pre Opportunity'
                                , '10-Duplicate'
                                )
            THEN TRUE
        ELSE FALSE
      END                                                                         AS is_net_arr_pipeline_created

    FROM sfdc_opportunity

), is_win_rate_calc AS (

    SELECT
        
      opportunity_id,
      CASE
        WHEN stage_name IN ('Closed Won', '8-Closed Lost')
          AND amount >= 0
          AND (reason_for_loss IS NULL OR reason_for_loss != 'Merged into another opportunity')
          AND is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                         AS is_win_rate_calc

    FROM sfdc_opportunity

), final_opportunities AS (

    SELECT

      -- opportunity and person ids
      opportunity_fields.dim_crm_opportunity_id,
      opportunity_fields.merged_crm_opportunity_id,
      opportunity_fields.dim_crm_account_id,
      crm_account_dimensions.dim_parent_crm_account_id,
      first_contact.dim_crm_person_id,
      first_contact.sfdc_contact_id,

      -- dates
      opportunity_fields.created_date,
      opportunity_fields.created_date_id,
      opportunity_fields.sales_accepted_date,
      opportunity_fields.sales_accepted_date_id,
      opportunity_fields.close_date,
      opportunity_fields.close_date_id,
      opportunity_fields.stage_0_pending_acceptance_date,
      opportunity_fields.stage_0_pending_acceptance_date_id,
      opportunity_fields.stage_1_discovery_date,
      opportunity_fields.stage_1_discovery_date_id,
      opportunity_fields.stage_2_scoping_date,
      opportunity_fields.stage_2_scoping_date_id,
      opportunity_fields.stage_3_technical_evaluation_date,
      opportunity_fields.stage_3_technical_evaluation_date_id,
      opportunity_fields.stage_4_proposal_date,
      opportunity_fields.stage_4_proposal_date_id,
      opportunity_fields.stage_5_negotiating_date,
      opportunity_fields.stage_5_negotiating_date_id,
      opportunity_fields.stage_6_closed_won_date,
      opportunity_fields.stage_6_closed_won_date_id,
      opportunity_fields.stage_6_closed_lost_date,
      opportunity_fields.stage_6_closed_lost_date_id,
      opportunity_fields.days_in_0_pending_acceptance,
      opportunity_fields.days_in_1_discovery,
      opportunity_fields.days_in_2_scoping,
      opportunity_fields.days_in_3_technical_evaluation,
      opportunity_fields.days_in_4_proposal,
      opportunity_fields.days_in_5_negotiating,
      opportunity_fields.days_in_sao,
      CASE
        WHEN opportunity_fields.days_in_sao < 0                  THEN '1. Closed in < 0 days'
        WHEN opportunity_fields.days_in_sao BETWEEN 0 AND 30     THEN '2. Closed in 0-30 days'
        WHEN opportunity_fields.days_in_sao BETWEEN 31 AND 60    THEN '3. Closed in 31-60 days'
        WHEN opportunity_fields.days_in_sao BETWEEN 61 AND 90    THEN '4. Closed in 61-90 days'
        WHEN opportunity_fields.days_in_sao BETWEEN 91 AND 180   THEN '5. Closed in 91-180 days'
        WHEN opportunity_fields.days_in_sao BETWEEN 181 AND 270  THEN '6. Closed in 181-270 days'
        WHEN opportunity_fields.days_in_sao > 270                THEN '7. Closed in > 270 days'
        ELSE NULL
      END                                                                                                                   AS closed_buckets,
      opportunity_fields.subscription_start_date,
      opportunity_fields.subscription_end_date,

      -- common dimension keys
      {{ get_keyed_nulls('opportunity_fields.dim_crm_sales_rep_id') }}                                                      AS dim_crm_sales_rep_id,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                                 AS dim_order_type_id,
      {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}                                         AS dim_sales_qualified_source_id,
      {{ get_keyed_nulls('deal_path.dim_deal_path_id') }}                                                                   AS dim_deal_path_id,
      {{ get_keyed_nulls('crm_account_dimensions.dim_parent_sales_segment_id,sales_segment.dim_sales_segment_id') }}        AS dim_parent_sales_segment_id,
      crm_account_dimensions.dim_parent_sales_territory_id,
      crm_account_dimensions.dim_parent_industry_id,
      crm_account_dimensions.dim_parent_location_country_id,
      crm_account_dimensions.dim_parent_location_region_id,
      {{ get_keyed_nulls('crm_account_dimensions.dim_account_sales_segment_id,sales_segment.dim_sales_segment_id') }}       AS dim_account_sales_segment_id,
      crm_account_dimensions.dim_account_sales_territory_id,
      crm_account_dimensions.dim_account_industry_id,
      crm_account_dimensions.dim_account_location_country_id,
      crm_account_dimensions.dim_account_location_region_id,
      {{ get_keyed_nulls('user_hierarchy_stamped_sales_segment.dim_crm_opp_owner_sales_segment_stamped_id') }}              AS dim_crm_opp_owner_sales_segment_stamped_id,
      {{ get_keyed_nulls('user_hierarchy_stamped_geo.dim_crm_opp_owner_geo_stamped_id') }}                                  AS dim_crm_opp_owner_geo_stamped_id,
      {{ get_keyed_nulls('user_hierarchy_stamped_region.dim_crm_opp_owner_region_stamped_id') }}                            AS dim_crm_opp_owner_region_stamped_id,
      {{ get_keyed_nulls('user_hierarchy_stamped_area.dim_crm_opp_owner_area_stamped_id') }}                                AS dim_crm_opp_owner_area_stamped_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_sales_segment_id') }}                                                      AS dim_crm_user_sales_segment_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_geo_id') }}                                                                AS dim_crm_user_geo_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_region_id') }}                                                             AS dim_crm_user_region_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_area_id') }}                                                               AS dim_crm_user_area_id,

            -- flags
      opportunity_fields.is_closed,
      opportunity_fields.is_won,
      opportunity_fields.is_refund,
      opportunity_fields.is_downgrade,
      opportunity_fields.is_swing_deal,
      opportunity_fields.is_edu_oss,
      opportunity_fields.is_web_portal_purchase,
      is_sao.is_sao,
      is_sdr_sao.is_sdr_sao,
      is_net_arr_closed_deal.is_net_arr_closed_deal,
      is_new_logo_first_order.is_new_logo_first_order,
      is_net_arr_pipeline_created.is_net_arr_pipeline_created,
      is_win_rate_calc.is_win_rate_calc,

      opportunity_fields.primary_solution_architect,
      opportunity_fields.product_details,

      -- channel fields
      opportunity_fields.lead_source,
      opportunity_fields.dr_partner_deal_type,
      opportunity_fields.dr_partner_engagement,
      opportunity_fields.partner_account,
      opportunity_fields.dr_status,
      opportunity_fields.distributor,
      opportunity_fields.influence_partner,
      opportunity_fields.fulfillment_partner,
      opportunity_fields.platform_partner,
      opportunity_fields.partner_track,
      opportunity_fields.is_public_sector_opp,
      opportunity_fields.is_registration_from_portal,
      opportunity_fields.calculated_discount,
      opportunity_fields.partner_discount,
      opportunity_fields.partner_discount_calc,
      opportunity_fields.comp_channel_neutral,

      -- additive fields
      opportunity_fields.iacv,
      opportunity_fields.net_arr,
      opportunity_fields.amount,
      opportunity_fields.recurring_amount,
      opportunity_fields.true_up_amount,
      opportunity_fields.proserv_amount,
      opportunity_fields.other_non_recurring_amount,
      opportunity_fields.arr_basis,
      opportunity_fields.arr,
      linear_attribution_base.count_crm_attribution_touchpoints,
      opportunity_fields.iacv/linear_attribution_base.count_crm_attribution_touchpoints                                        AS weighted_linear_iacv,
      campaigns_per_opp.count_campaigns

    FROM opportunity_fields
    LEFT JOIN crm_account_dimensions
      ON opportunity_fields.dim_crm_account_id = crm_account_dimensions.dim_crm_account_id
    LEFT JOIN first_contact
      ON opportunity_fields.dim_crm_opportunity_id = first_contact.opportunity_id AND first_contact.row_num = 1
    LEFT JOIN sales_qualified_source
      ON opportunity_fields.sales_qualified_source = sales_qualified_source.sales_qualified_source_name
    LEFT JOIN order_type
      ON opportunity_fields.order_type = order_type.order_type_name
    LEFT JOIN deal_path
      ON opportunity_fields.deal_path = deal_path.deal_path_name
    LEFT JOIN sales_segment
      ON opportunity_fields.sales_segment = sales_segment.sales_segment_name
    LEFT JOIN is_sao
      ON opportunity_fields.dim_crm_opportunity_id = is_sao.opportunity_id
    LEFT JOIN is_sdr_sao
      ON opportunity_fields.dim_crm_opportunity_id = is_sdr_sao.opportunity_id
    LEFT JOIN is_net_arr_closed_deal
      ON opportunity_fields.dim_crm_opportunity_id = is_net_arr_closed_deal.opportunity_id
    LEFT JOIN is_new_logo_first_order
      ON opportunity_fields.dim_crm_opportunity_id = is_new_logo_first_order.opportunity_id
    LEFT JOIN is_net_arr_pipeline_created
      ON opportunity_fields.dim_crm_opportunity_id = is_net_arr_pipeline_created.opportunity_id
    LEFT JOIN is_win_rate_calc
      ON opportunity_fields.dim_crm_opportunity_id = is_win_rate_calc.opportunity_id
    LEFT JOIN user_hierarchy_stamped_sales_segment
      ON opportunity_fields.crm_opp_owner_sales_segment_stamped = user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped
    LEFT JOIN user_hierarchy_stamped_geo
      ON opportunity_fields.crm_opp_owner_geo_stamped = user_hierarchy_stamped_geo.crm_opp_owner_geo_stamped
    LEFT JOIN user_hierarchy_stamped_region
      ON opportunity_fields.crm_opp_owner_region_stamped = user_hierarchy_stamped_region.crm_opp_owner_region_stamped
    LEFT JOIN user_hierarchy_stamped_area
      ON opportunity_fields.crm_opp_owner_area_stamped = user_hierarchy_stamped_area.crm_opp_owner_area_stamped
    LEFT JOIN sales_rep
      ON opportunity_fields.dim_crm_sales_rep_id = sales_rep.dim_crm_sales_rep_id
    LEFT JOIN linear_attribution_base
      ON opportunity_fields.dim_crm_opportunity_id = linear_attribution_base.dim_crm_opportunity_id
    LEFT JOIN campaigns_per_opp
      ON opportunity_fields.dim_crm_opportunity_id = campaigns_per_opp.dim_crm_opportunity_id

)

{{ dbt_audit(
    cte_ref="final_opportunities",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-11-30",
    updated_date="2021-03-25"
) }}
