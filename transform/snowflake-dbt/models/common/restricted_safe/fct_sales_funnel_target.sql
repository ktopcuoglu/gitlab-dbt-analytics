WITH date AS (

   SELECT DISTINCT
     fiscal_month_name_fy,
     fiscal_year,
     first_day_of_month
   FROM {{ ref('date_details_source') }}

), sales_qualified_source AS (

    SELECT *
    FROM {{ ref('prep_sales_qualified_source') }}

), order_type AS (

    SELECT *
    FROM {{ ref('prep_order_type') }}

), sfdc_user_hierarchy_live AS (

    SELECT *
    FROM {{ ref('prep_crm_user_hierarchy_live') }}

), sfdc_user_hierarchy_stamped AS (

    SELECT *
    FROM {{ ref('prep_crm_user_hierarchy_stamped') }}

), target_matrix AS (

    SELECT
      sheetload_sales_funnel_targets_matrix_source.*,
      date.first_day_of_month,
      date.fiscal_year,
      {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}   AS dim_sales_qualified_source_id,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                           AS dim_order_type_id,
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source' )}}
    LEFT JOIN date
      ON {{ sales_funnel_text_slugify("target_matrix.month") }} = {{ sales_funnel_text_slugify("date.fiscal_month_name_fy") }}
    LEFT JOIN sales_qualified_source
      ON {{ sales_funnel_text_slugify("target_matrix.opportunity_source") }} = {{ sales_funnel_text_slugify("sales_qualified_source.sales_qualified_source_name") }}
    LEFT JOIN order_type
      ON {{ sales_funnel_text_slugify("target_matrix.order_type") }} = {{ sales_funnel_text_slugify("order_type.order_type_name") }}

), fy22_user_hierarchy AS (

    SELECT *
    FROM sfdc_user_hierarchy_stamped
    WHERE fiscal_year = 2022
      AND is_last_user_hierarchy_in_fiscal_year = 1

), fy23_and_beyond_user_hierarchy AS (

    SELECT *
    FROM sfdc_user_hierarchy_stamped
    WHERe fiscal_year > 2022
      AND (
            is_last_user_hierarchy_in_fiscal_year = 1
              OR 
                is_last_user_area_in_fiscal_year = 1
           )

), unioned_targets AS (

    SELECT *
    FROM target_matrix
    LEFT JOIN fy22_user_hierarchy
      ON target_matrix.area = fy22_user_hierarchy.crm_opp_owner_area_stamped
    WHERE target_matrix.fiscal_year = 2022 

    UNION ALL

    SELECT *
    FROM target_matrix
    LEFT JOIN fy23_and_beyond_user_hierarchy
      ON target_matrix.area = fy23_and_beyond_user_hierarchy.crm_opp_owner_user_segment_geo_region_area_stamped
    WHERE target_matrix.fiscal_year > 2022 

), final_targets AS (

     SELECT

     {{ dbt_utils.surrogate_key([
                                 'unioned_targets.kpi_name',
                                 'unioned_targets.first_day_of_month', 
                                 'unioned_targets.dim_sales_qualified_source_id',
                                 'unioned_targets.dim_order_type_id', 
                                 'unioned_targets.dim_crm_user_hierarchy_stamped_id',
                                 'unioned_targets.dim_crm_user_sales_segment_stamped_id',
                                 'unioned_targets.dim_crm_user_geo_stamped_id', 
                                 'unioned_targets.dim_crm_user_region_stamped_id', 
                                 'unioned_targets.dim_crm_user_area_stamped_id'
                                 ]) 
     }}
                                                                                     AS sales_funnel_target_id,
     target_matrix.kpi_name,
     date.first_day_of_month,
     target_matrix.opportunity_source                                                AS sales_qualified_source,
     {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}   AS dim_sales_qualified_source_id,
     target_matrix.order_type,
     order_type.dim_order_type_id,
     sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id,
     sfdc_user_hierarchy_live.dim_crm_user_sales_segment_id,
     sfdc_user_hierarchy_live.dim_crm_user_geo_id,
     sfdc_user_hierarchy_live.dim_crm_user_region_id,
     sfdc_user_hierarchy_live.dim_crm_user_area_id,
     unioned_targets.dim_crm_user_hierarchy_stamped_id,
     unioned_targets.dim_crm_opp_owner_sales_segment_stamped_id,
     unioned_targets.dim_crm_opp_owner_geo_stamped_id,
     unioned_targets.dim_crm_opp_owner_region_stamped_id,
     unioned_targets.dim_crm_opp_owner_area_stamped_id,
     SUM(target_matrix.allocated_target)                                             AS allocated_target

    FROM unioned_targets
    LEFT JOIN sfdc_user_hierarchy_live
      ON unioned_targets.dim_crm_user_hierarchy_stamped_id = s  
    {{ dbt_utils.group_by(n=17) }}

)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@mcooperDD",
    updated_by="@jpeguero",
    created_date="2020-12-18",
    updated_date="2021-05-06"
) }}
