  
  {{ config(alias='mart_sales_funnel_target_daily') }}


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
          END                                                       AS deal_group,
          
          COALESCE(funnel_target.sales_qualified_source_name,'NA')  AS sales_qualified_source,
          LOWER(CONCAT(funnel_target.crm_user_sales_segment,'-',funnel_target.crm_user_geo,'-',funnel_target.crm_user_region,'-',funnel_target.crm_user_area, '-', sales_qualified_source, '-',funnel_target.order_type_name)) AS report_user_segment_geo_region_area_sqs_ot
    FROM {{ref('mart_sales_funnel_target_daily')}} funnel_target
  
  ), agg_demo_keys AS (
  -- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }} 

  
  ), final AS (

    SELECT
          funnel_target.*,
          target_date.fiscal_quarter_name_fy           AS target_fiscal_quarter_name,
          target_date.first_day_of_fiscal_quarter      AS target_fiscal_quarter_date, 
          target_date.day_of_fiscal_quarter_normalised AS target_day_of_fiscal_quarter_normalised,  

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
      INNER JOIN  date_details target_date
        ON target_date.date_actual = funnel_target.target_date
      LEFT JOIN agg_demo_keys
        ON funnel_target.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    WHERE LOWER(funnel_target.deal_group) LIKE ANY ('%growth%','%new%')
  )

  SELECT *
  FROM final