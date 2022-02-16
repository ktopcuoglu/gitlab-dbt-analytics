  
  {{ config(alias='mart_sales_funnel_target') }}


  WITH date_details AS (
    
    SELECT *
    FROM {{ ref('wk_sales_date_details') }}  

  ), mart_sales_funnel_target AS (

    SELECT *
    FROM {{ref('mart_sales_funnel_target')}} 
  
  ), final AS (

    SELECT
          funnel_target.*,
          target_month.fiscal_quarter_name_fy           AS target_fiscal_quarter_name,
          target_month.first_day_of_fiscal_quarter      AS target_fiscal_quarter_date,   

          COALESCE(funnel_target.sales_qualified_source_name,'NA')                                              AS sales_qualified_source,
          ---------
          -- 2022-02-01 NF Deprecated, this should be removed once the Q1 clean up exercise is done
          CASE 
            WHEN funnel_target.crm_user_sales_segment = 'Large'
              AND funnel_target.crm_user_geo  = 'EMEA'
                THEN 'Large_EMEA'
            WHEN funnel_target.crm_user_sales_segment = 'Mid-Market'
              AND funnel_target.crm_user_region  = 'AMER'
              AND lower(funnel_target.crm_user_area) LIKE '%west%'
                THEN 'Mid-Market_West'
            WHEN funnel_target.crm_user_sales_segment = 'Mid-Market'
              AND funnel_target.crm_user_region = 'AMER'
              AND lower(funnel_target.crm_user_area) NOT LIKE '%west%'
                THEN 'Mid-Market_East'
            WHEN funnel_target.crm_user_sales_segment = 'SMB'
              AND funnel_target.crm_user_region = 'AMER'
              AND lower(funnel_target.crm_user_area) LIKE '%west%'
                THEN 'SMB_West'
            WHEN funnel_target.crm_user_sales_segment = 'SMB'
              AND funnel_target.crm_user_region = 'AMER'
              AND lower(funnel_target.crm_user_area) NOT LIKE '%west%'
                THEN 'SMB_East'
            ELSE COALESCE(CONCAT(funnel_target.crm_user_sales_segment,'_',funnel_target.crm_user_region),'NA') 
        END                                                                                           AS sales_team_rd_asm_level,
          ---------

          COALESCE(funnel_target.crm_user_sales_segment ,'NA')                                        AS sales_team_cro_level,
          COALESCE(CONCAT(funnel_target.crm_user_sales_segment,'_',funnel_target.crm_user_geo),'NA')  AS sales_team_vp_level,
          COALESCE(CONCAT(funnel_target.crm_user_sales_segment,
            '_',funnel_target.crm_user_geo,'_',funnel_target.crm_user_region),'NA')                   AS sales_team_avp_rd_level,
          COALESCE(CONCAT(funnel_target.crm_user_sales_segment,'_',funnel_target.crm_user_geo,
            '_',funnel_target.crm_user_region,'_',funnel_target.crm_user_area),'NA')                  AS sales_team_asm_level,

          -- 20220214 NF: Temporary keys, until the SFDC key is exposed
          LOWER(CONCAT(funnel_target.crm_user_sales_segment,'-',funnel_target.crm_user_geo,'-',funnel_target.crm_user_region,'-',funnel_target.crm_user_area))   AS report_user_segment_geo_region_area,

          CASE 
            WHEN funnel_target.order_type_name = '3. Growth' 
                THEN '2. Growth'
            WHEN funnel_target.order_type_name = '1. New - First Order' 
                THEN '1. New'
              ELSE '3. Other'
          END                                                AS deal_group
    FROM mart_sales_funnel_target funnel_target
      INNER JOIN  date_details target_month
      ON target_month.date_actual = funnel_target.target_month	
    WHERE LOWER(deal_group) LIKE ANY ('%growth%','%new%')
  )

  SELECT *
  FROM final