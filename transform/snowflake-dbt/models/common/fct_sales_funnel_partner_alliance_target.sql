{{ simple_cte([
    ('sales_qualified_source', 'prep_sales_qualified_source'),
    ('order_type', 'prep_order_type'),
    ('sfdc_user_hierarchy_live','prep_crm_user_hierarchy_live'),
    ('sfdc_user_hierarchy_stamped', 'prep_crm_user_hierarchy_stamped'),
    ('target_matrix', 'sheetload_sales_funnel_partner_alliance_targets_matrix_source'),
    ('dr_partner_engagement', 'prep_dr_partner_engagement'),
    ('alliance_type', 'prep_alliance_type'),
    ('channel_type', 'prep_channel_type')
]) }},

date AS (

   SELECT DISTINCT
     fiscal_month_name_fy,
     first_day_of_month
   FROM {{ ref('date_details_source') }}

), target_matrix_intermediate AS (

    SELECT
      *,
      {{ channel_type('target_matrix.partner_engagement_type', 'target_matrix.order_type') }}  AS channel_type
    FROM target_matrix

), final_targets AS (

  SELECT

    {{ dbt_utils.surrogate_key([
                                'target_matrix_intermediate.kpi_name',
                                'date.first_day_of_month',
                                'order_type.dim_order_type_id',
                                'sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id',
                                'sfdc_user_hierarchy_live.dim_crm_user_sales_segment_id',
                                'sfdc_user_hierarchy_live.dim_crm_user_geo_id',
                                'sfdc_user_hierarchy_live.dim_crm_user_region_id',
                                'sfdc_user_hierarchy_live.dim_crm_user_area_id',
                                'dr_partner_engagement.dim_dr_partner_engagement_id',
                                'alliance_type.dim_alliance_type_id',
                                'channel_type.dim_channel_type_id'
                                ])
    }}
                                                                                    AS sales_funnel_partner_alliance_target_id,
    target_matrix_intermediate.kpi_name,
    date.first_day_of_month,
    target_matrix_intermediate.partner_engagement_type                              AS dr_partner_engagement,
    dr_partner_engagement.dim_dr_partner_engagement_id,
    target_matrix_intermediate.alliance_partner                                     AS alliance_type,
    {{ get_keyed_nulls('alliance_type.dim_alliance_type_id') }}                     AS dim_alliance_type_id,
    target_matrix_intermediate.order_type,
    order_type.dim_order_type_id,
    channel_type,
    {{ get_keyed_nulls('channel_type.dim_channel_type_id') }}                       AS dim_channel_type_id,           
    sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id,
    sfdc_user_hierarchy_live.dim_crm_user_sales_segment_id,
    sfdc_user_hierarchy_live.dim_crm_user_geo_id,
    sfdc_user_hierarchy_live.dim_crm_user_region_id,
    sfdc_user_hierarchy_live.dim_crm_user_area_id,
    sfdc_user_hierarchy_stamped.dim_crm_user_hierarchy_stamped_id,
    sfdc_user_hierarchy_stamped.dim_crm_opp_owner_sales_segment_stamped_id,
    sfdc_user_hierarchy_stamped.dim_crm_opp_owner_geo_stamped_id,
    sfdc_user_hierarchy_stamped.dim_crm_opp_owner_region_stamped_id,
    sfdc_user_hierarchy_stamped.dim_crm_opp_owner_area_stamped_id,
    SUM(target_matrix_intermediate.allocated_target)                                             AS allocated_target

  FROM target_matrix_intermediate
  LEFT JOIN sfdc_user_hierarchy_live
    ON {{ sales_funnel_text_slugify("target_matrix_intermediate.area") }} = {{ sales_funnel_text_slugify("sfdc_user_hierarchy_live.crm_user_area") }}
  LEFT JOIN date
    ON {{ sales_funnel_text_slugify("target_matrix_intermediate.month") }} = {{ sales_funnel_text_slugify("date.fiscal_month_name_fy") }}
  LEFT JOIN dr_partner_engagement
    ON {{ sales_funnel_text_slugify("target_matrix_intermediate.partner_engagement_type") }} = {{ sales_funnel_text_slugify("dr_partner_engagement.dr_partner_engagement_name") }}
  LEFT JOIN alliance_type
    ON {{ sales_funnel_text_slugify("target_matrix_intermediate.alliance_partner") }} = {{ sales_funnel_text_slugify("alliance_type.alliance_type_name") }}
  LEFT JOIN channel_type
    ON {{ sales_funnel_text_slugify("target_matrix_intermediate.channel_type") }} = {{ sales_funnel_text_slugify("channel_type.channel_type_name") }}
  LEFT JOIN order_type
    ON {{ sales_funnel_text_slugify("target_matrix_intermediate.order_type") }} = {{ sales_funnel_text_slugify("order_type.order_type_name") }}
  LEFT JOIN sfdc_user_hierarchy_stamped
    ON sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id = sfdc_user_hierarchy_stamped.dim_crm_user_hierarchy_stamped_id
  {{ dbt_utils.group_by(n=21) }}

)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@jpeguero",
    updated_by="@iweeks",
    created_date="2021-04-08",
    updated_date="2021-04-28"
) }}
