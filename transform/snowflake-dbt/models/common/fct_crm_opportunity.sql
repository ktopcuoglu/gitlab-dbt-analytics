{{ simple_cte([
    ('crm_account_dimensions', 'map_crm_account'),
    ('order_type', 'prep_order_type'),
    ('sales_qualified_source', 'prep_sales_qualified_source'),
    ('deal_path', 'prep_deal_path'),
    ('sales_rep', 'prep_crm_user'),
    ('sales_segment', 'prep_sales_segment'),
    ('sfdc_campaigns', 'prep_campaign'),
    ('dr_partner_engagement', 'prep_dr_partner_engagement'),
    ('alliance_type', 'prep_alliance_type'),
    ('channel_type', 'prep_channel_type')

]) }}

, sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL

), first_contact  AS (

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

      sfdc_opportunity.opportunity_id                                            AS dim_crm_opportunity_id,
      sfdc_opportunity.merged_opportunity_id                                     AS merged_crm_opportunity_id,
      sfdc_opportunity.account_id                                                AS dim_crm_account_id,
      sfdc_opportunity.owner_id                                                  AS dim_crm_sales_rep_id,
      sfdc_opportunity.incremental_acv                                           AS iacv,
      sfdc_opportunity.net_arr,
      sfdc_opportunity.amount,
      sfdc_opportunity.recurring_amount,
      sfdc_opportunity.true_up_amount,
      sfdc_opportunity.proserv_amount,
      sfdc_opportunity.other_non_recurring_amount,
      sfdc_opportunity.arr_basis,
      sfdc_opportunity.arr,
      sfdc_opportunity.subscription_start_date,
      sfdc_opportunity.subscription_end_date,
      sfdc_opportunity.created_date::DATE                                        AS created_date,
      {{ get_date_id('sfdc_opportunity.created_date') }}                         AS created_date_id,
      sfdc_opportunity.sales_accepted_date::DATE                                 AS sales_accepted_date,
      {{ get_date_id('sfdc_opportunity.sales_accepted_date') }}                  AS sales_accepted_date_id,
      sfdc_opportunity.close_date::DATE                                          AS close_date,
      {{ get_date_id('sfdc_opportunity.close_date') }}                           AS close_date_id,
      sfdc_opportunity.stage_0_pending_acceptance_date::DATE                     AS stage_0_pending_acceptance_date,
      {{ get_date_id('sfdc_opportunity.stage_0_pending_acceptance_date') }}      AS stage_0_pending_acceptance_date_id,
      sfdc_opportunity.stage_1_discovery_date::DATE                              AS stage_1_discovery_date,
      {{ get_date_id('sfdc_opportunity.stage_1_discovery_date') }}               AS stage_1_discovery_date_id,
      sfdc_opportunity.stage_2_scoping_date::DATE                                AS stage_2_scoping_date,
      {{ get_date_id('sfdc_opportunity.stage_2_scoping_date') }}                 AS stage_2_scoping_date_id,
      sfdc_opportunity.stage_3_technical_evaluation_date::DATE                   AS stage_3_technical_evaluation_date,
      {{ get_date_id('sfdc_opportunity.stage_3_technical_evaluation_date') }}    AS stage_3_technical_evaluation_date_id,
      sfdc_opportunity.stage_4_proposal_date::DATE                               AS stage_4_proposal_date,
      {{ get_date_id('sfdc_opportunity.stage_4_proposal_date') }}                AS stage_4_proposal_date_id,
      sfdc_opportunity.stage_5_negotiating_date::DATE                            AS stage_5_negotiating_date,
      {{ get_date_id('sfdc_opportunity.stage_5_negotiating_date') }}             AS stage_5_negotiating_date_id,
      sfdc_opportunity.stage_6_closed_won_date::DATE                             AS stage_6_closed_won_date,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_won_date') }}              AS stage_6_closed_won_date_id,
      sfdc_opportunity.stage_6_closed_lost_date::DATE                            AS stage_6_closed_lost_date,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_lost_date') }}             AS stage_6_closed_lost_date_id,
      sfdc_opportunity.days_in_0_pending_acceptance,
      sfdc_opportunity.days_in_1_discovery,
      sfdc_opportunity.days_in_2_scoping,
      sfdc_opportunity.days_in_3_technical_evaluation,
      sfdc_opportunity.days_in_4_proposal,
      sfdc_opportunity.days_in_5_negotiating,
      sfdc_opportunity.is_closed,
      sfdc_opportunity.is_won,
      sfdc_opportunity.is_refund,
      sfdc_opportunity.is_downgrade,
      sfdc_opportunity.is_swing_deal,
      sfdc_opportunity.is_edu_oss,
      sfdc_opportunity.is_web_portal_purchase,
      sfdc_opportunity.deal_path,
      sfdc_opportunity.order_type_stamped                                        AS order_type,
      sfdc_opportunity.sales_segment,
      {{ sales_qualified_source_cleaning('sfdc_opportunity.sales_qualified_source') }}
                                                                                 AS sales_qualified_source,
      sfdc_opportunity.days_in_sao,
      sfdc_opportunity.user_segment_stamped                                      AS crm_opp_owner_sales_segment_stamped,
      sfdc_opportunity.user_geo_stamped                                          AS crm_opp_owner_geo_stamped,
      sfdc_opportunity.user_region_stamped                                       AS crm_opp_owner_region_stamped,
      sfdc_opportunity.user_area_stamped                                         AS crm_opp_owner_area_stamped,
      sfdc_opportunity.primary_solution_architect,
      sfdc_opportunity.product_details,
      sfdc_opportunity.product_category,
      sfdc_opportunity.products_purchased,
      sfdc_opportunity.dr_partner_deal_type,
      sfdc_opportunity.dr_partner_engagement,
      {{ alliance_type('partner_account.account_name', 'influence_partner.account_name', 'sfdc_opportunity.partner_account', 'sfdc_opportunity.influence_partner') }},
      {{ alliance_type_short('partner_account.account_name', 'influence_partner.account_name', 'sfdc_opportunity.partner_account', 'sfdc_opportunity.influence_partner') }},
      sfdc_opportunity.channel_type,
      sfdc_opportunity.partner_account,
      sfdc_opportunity.dr_status,
      sfdc_opportunity.distributor,
      sfdc_opportunity.influence_partner,
      sfdc_opportunity.fulfillment_partner,
      sfdc_opportunity.platform_partner,
      sfdc_opportunity.partner_track,
      sfdc_opportunity.is_public_sector_opp,
      sfdc_opportunity.is_registration_from_portal,
      sfdc_opportunity.calculated_discount,
      sfdc_opportunity.partner_discount,
      sfdc_opportunity.partner_discount_calc,
      sfdc_opportunity.comp_channel_neutral,
      sfdc_opportunity.lead_source

    FROM sfdc_opportunity
    LEFT JOIN sfdc_account AS partner_account
      ON sfdc_opportunity.account_id = partner_account.account_id
    LEFT JOIN sfdc_account AS influence_partner
      ON sfdc_opportunity.account_id = influence_partner.account_id

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
      {{ get_keyed_nulls('opportunity_fields.dim_crm_user_id') }}                                                           AS dim_crm_user_id,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                                 AS dim_order_type_id,
      {{ get_keyed_nulls('dr_partner_engagement.dim_dr_partner_engagement_id') }}                                           AS dim_dr_partner_engagement_id,
      {{ get_keyed_nulls('alliance_type.dim_alliance_type_id') }}                                                           AS dim_alliance_type_id,
      {{ get_keyed_nulls('channel_type.dim_channel_type_id') }}                                                             AS dim_channel_type_id,
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

      opportunity_fields.primary_solution_architect,
      opportunity_fields.product_details,
      opportunity_fields.product_category,
      opportunity_fields.products_purchased,

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
    LEFT JOIN user_hierarchy_stamped_sales_segment
      ON opportunity_fields.crm_opp_owner_sales_segment_stamped = user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped
    LEFT JOIN user_hierarchy_stamped_geo
      ON opportunity_fields.crm_opp_owner_geo_stamped = user_hierarchy_stamped_geo.crm_opp_owner_geo_stamped
    LEFT JOIN user_hierarchy_stamped_region
      ON opportunity_fields.crm_opp_owner_region_stamped = user_hierarchy_stamped_region.crm_opp_owner_region_stamped
    LEFT JOIN user_hierarchy_stamped_area
      ON opportunity_fields.crm_opp_owner_area_stamped = user_hierarchy_stamped_area.crm_opp_owner_area_stamped
    LEFT JOIN dr_partner_engagement
      ON opportunity_fields.dr_partner_engagement = dr_partner_engagement.dr_partner_engagement_name
    LEFT JOIN alliance_type
      ON opportunity_fields.alliance_type = alliance_type.alliance_type_name
    LEFT JOIN channel_type
      ON opportunity_fields.alliance_type = channel_type.channel_type_name
    LEFT JOIN sales_rep
      ON opportunity_fields.dim_crm_user_id = sales_rep.dim_crm_user_id
    LEFT JOIN linear_attribution_base
      ON opportunity_fields.dim_crm_opportunity_id = linear_attribution_base.dim_crm_opportunity_id
    LEFT JOIN campaigns_per_opp
      ON opportunity_fields.dim_crm_opportunity_id = campaigns_per_opp.dim_crm_opportunity_id

)

{{ dbt_audit(
    cte_ref="final_opportunities",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2020-11-30",
    updated_date="2021-04-28"
) }}
