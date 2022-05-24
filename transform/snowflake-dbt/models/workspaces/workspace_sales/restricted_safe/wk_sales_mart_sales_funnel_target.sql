  
  {{ config(alias='mart_sales_funnel_target') }}


  WITH date_details AS (
    
    SELECT *
    FROM {{ ref('wk_sales_date_details') }}  

  ), mart_sales_funnel_target AS (

    SELECT 
          funnel_target.*,
          -- 20220214 NF: Temporary keys, until the SFDC key is exposed,
          CASE 
            WHEN funnel_target.order_type_name = '3. Growth' 
                THEN '2. Growth'
            WHEN funnel_target.order_type_name = '1. New - First Order' 
                THEN '1. New'
              ELSE '3. Other'
          END                                                AS deal_group,
          
          COALESCE(funnel_target.sales_qualified_source_name,'NA')                                              AS sales_qualified_source,
          LOWER(CONCAT(funnel_target.crm_user_sales_segment,'-',funnel_target.crm_user_geo,'-',funnel_target.crm_user_region,'-',funnel_target.crm_user_area, '-', sales_qualified_source, '-',funnel_target.order_type_name)) AS report_user_segment_geo_region_area_sqs_ot
    FROM {{ref('mart_sales_funnel_target')}} funnel_target

  
  ), agg_demo_keys AS (
  -- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }} 

  
  ), final AS (

    SELECT
          funnel_target.*,
          target_month.fiscal_quarter_name_fy           AS target_fiscal_quarter_name,
          target_month.first_day_of_fiscal_quarter      AS target_fiscal_quarter_date,   
          target_month.fiscal_year                      AS target_fiscal_year,

          agg_demo_keys.sales_team_cro_level,
          agg_demo_keys.sales_team_vp_level,
          agg_demo_keys.sales_team_avp_rd_level,
          agg_demo_keys.sales_team_asm_level,
          agg_demo_keys.sales_team_rd_asm_level,

          agg_demo_keys.key_segment,
          agg_demo_keys.key_sqs,
          agg_demo_keys.key_ot,

          agg_demo_keys.key_segment_geo,
          agg_demo_keys.key_segment_geo_sqs,
          agg_demo_keys.key_segment_geo_ot,      

          agg_demo_keys.key_segment_geo_region,
          agg_demo_keys.key_segment_geo_region_sqs,
          agg_demo_keys.key_segment_geo_region_ot,   

          agg_demo_keys.key_segment_geo_region_area,
          agg_demo_keys.key_segment_geo_region_area_sqs,
          agg_demo_keys.key_segment_geo_region_area_ot,

          agg_demo_keys.report_user_segment_geo_region_area


    FROM mart_sales_funnel_target funnel_target
      INNER JOIN  date_details target_month
        ON target_month.date_actual = funnel_target.target_month
      LEFT JOIN agg_demo_keys
        ON funnel_target.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    WHERE LOWER(funnel_target.deal_group) LIKE ANY ('%growth%','%new%')
  
  ), fy24_target_placeholder AS (
   
   /*
     2022-05-23 NF:

     To keep the X-Ray dashboard & Pipeline Velocity tools useful after Q2 FY23, we need to set up FY24 placeholders.

     The issue is here: https://gitlab.com/gitlab-com/sales-team/field-operations/analytics/-/issues/340

   */

    SELECT 
      final.sales_funnel_target_id,
      next_fy_date.date_actual            AS target_month,
      final.kpi_name,
      final.crm_user_sales_segment,
      final.crm_user_sales_segment_grouped,
      final.crm_user_geo,
      final.crm_user_region,
      final.crm_user_area,
      final.crm_user_sales_segment_region_grouped,
      final.order_type_name,
      final.order_type_grouped,
      final.sales_qualified_source_name,
      final.sales_qualified_source_grouped,
      -- Ratios provided by SS&A team members
      -- https://gitlab.com/gitlab-com/sales-team/field-operations/analytics/-/issues/340
      CASE
        WHEN final.key_segment IN ( 'large', 'pubsec' ) 
          THEN final.allocated_target * 1.62
        WHEN final.key_segment IN ( 'mid-market' ) 
          THEN final.allocated_target * 1.67
        WHEN final.key_segment IN ( 'smb' ) 
          THEN final.allocated_target * 1.9
        ELSE 0
      END                                 AS allocated_target,
      final.created_by,
      final.updated_by,
      final.model_created_date,
      final.model_updated_date,
      final.dbt_updated_at,
      final.dbt_created_at,
      final.deal_group,
      final.sales_qualified_source,
      report_user_segment_geo_region_area_sqs_ot,
      next_fy_date.fiscal_quarter_name_fy         AS target_fiscal_quarter_name,
      next_fy_date.first_day_of_fiscal_quarter    AS target_fiscal_quarter_date,
      next_fy_date.fiscal_year                    AS target_fiscal_year,
      final.sales_team_cro_level,
      final.sales_team_vp_level,
      final.sales_team_avp_rd_level,
      final.sales_team_asm_level,
      final.sales_team_rd_asm_level,
      final.key_segment,
      final.key_sqs,
      final.key_ot,
      final.key_segment_geo,
      final.key_segment_geo_sqs,
      final.key_segment_geo_ot,
      final.key_segment_geo_region,
      final.key_segment_geo_region_sqs,
      final.key_segment_geo_region_ot,
      final.key_segment_geo_region_area,
      final.key_segment_geo_region_area_sqs,
      final.key_segment_geo_region_area_ot,
      final.report_user_segment_geo_region_area
  FROM  final 
    INNER JOIN date_details target_date
      ON target_date.date_actual = final.target_month
    INNER JOIN date_details next_fy_date
      ON next_fy_date.date_actual = DATEADD(MONTH, 12, target_date.date_actual)
  WHERE  target_date.fiscal_year = 2023 -- Only relevant for FY24, Net ARR Targets * growth ratio
    AND final.kpi_name = 'Net ARR'

), final_with_placeholder AS (

  SELECT *
  FROM final
  UNION
  SELECT *
  FROM fy24_target_placeholder

)

SELECT *
FROM final_with_placeholder