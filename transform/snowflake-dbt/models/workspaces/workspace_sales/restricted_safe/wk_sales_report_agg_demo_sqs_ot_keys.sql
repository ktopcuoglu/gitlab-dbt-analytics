{{ config(alias='report_agg_demo_sqs_ot_keys') }}

WITH sfdc_account_xf AS (

    SELECT *
    FROM {{ref('sfdc_accounts_xf')}}

), sfdc_opportunity_xf AS ( 

    SELECT 
        
        opportunity.*,
        -- medium level grouping of the order type field
        CASE 
          WHEN opportunity.order_type_stamped = '1. New - First Order' 
            THEN '1. New'
          WHEN opportunity.order_type_stamped IN ('2. New - Connected', '3. Growth') 
            THEN '2. Growth' 
          WHEN opportunity.order_type_stamped IN ('4. Contraction')
            THEN '3. Contraction'
          WHEN opportunity.order_type_stamped IN ('5. Churn - Partial','6. Churn - Final')
            THEN '4. Churn'
          ELSE '5. Other' 
        END                                                                   AS deal_category,

        CASE 
          WHEN opportunity.order_type_stamped = '1. New - First Order' 
            THEN '1. New'
          WHEN opportunity.order_type_stamped IN ('2. New - Connected', '3. Growth', '5. Churn - Partial','6. Churn - Final','4. Contraction') 
            THEN '2. Growth' 
          ELSE '3. Other'
        END                                                                   AS deal_group,

        account.account_owner_user_segment,
        account.account_owner_user_geo, 
        account.account_owner_user_region,
        account.account_owner_user_area
  
    FROM {{ref('sfdc_opportunity_xf')}} opportunity
    INNER JOIN sfdc_account_xf account
        ON   account.account_id = opportunity.account_id


), eligible AS (

  SELECT         
        LOWER(user_segment_stamped)       AS report_opportunity_user_segment,
        LOWER(user_geo_stamped)           AS report_opportunity_user_geo,
        LOWER(user_region_stamped)        AS report_opportunity_user_region,
        LOWER(user_area_stamped)          AS report_opportunity_user_area,
        
        LOWER(sales_qualified_source)     AS sales_qualified_source,
        LOWER(order_type_stamped)         AS order_type_stamped,
  
        LOWER(deal_category)              AS deal_category,
        LOWER(deal_group)                 AS deal_group,
  
        LOWER(CONCAT(user_segment_stamped, '-',user_geo_stamped, '-',user_region_stamped, '-',user_area_stamped))                                                          AS report_user_segment_geo_region_area,
        LOWER(CONCAT(user_segment_stamped, '-',user_geo_stamped, '-',user_region_stamped, '-',user_area_stamped, '-', sales_qualified_source, '-', order_type_stamped))    AS report_user_segment_geo_region_area_sqs_ot
  FROM sfdc_opportunity_xf
  
  UNION
  
  SELECT         
        LOWER(account_owner_user_segment)       AS report_opportunity_user_segment,
        LOWER(account_owner_user_geo)           AS report_opportunity_user_geo,
        LOWER(account_owner_user_region)        AS report_opportunity_user_region,
        LOWER(account_owner_user_area)          AS report_opportunity_user_area,
        
        LOWER(sales_qualified_source)           AS sales_qualified_source,
        LOWER(order_type_stamped)               AS order_type_stamped,
  
        LOWER(deal_category)                    AS deal_category,
        LOWER(deal_group)                       AS deal_group,
  
        LOWER(CONCAT(account_owner_user_segment,'-',account_owner_user_geo,'-',account_owner_user_region,'-',account_owner_user_area))                                                          AS report_user_segment_geo_region_area,
        LOWER(CONCAT(account_owner_user_segment,'-',account_owner_user_geo,'-',account_owner_user_region,'-',account_owner_user_area, '-', sales_qualified_source, '-', order_type_stamped))    AS report_user_segment_geo_region_area_sqs_ot
  FROM sfdc_opportunity_xf
  
  
), valid_keys AS (

  SELECT DISTINCT 

        -- Segment
        -- Sales Qualified Source
        -- Order Type

        -- Segment - Geo
        -- Segment - Geo - Region

        -- Segment - Geo - Order Type Group 
        -- Segment - Geo - Sales Qualified Source

        -- Segment - Geo - Region - Order Type Group 
        -- Segment - Geo - Region - Sales Qualified Source
        -- Segment - Geo - Region - Area

        -- Segment - Geo - Region - Area - Order Type Group 
        -- Segment - Geo - Region - Area - Sales Qualified Source

        *,

        report_opportunity_user_segment   AS key_segment,
        sales_qualified_source            AS key_sqs,
        deal_group                        AS key_ot,

        report_opportunity_user_segment || '_' || report_opportunity_user_geo                                               AS key_segment_geo,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' ||  sales_qualified_source             AS key_segment_geo_sqs,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' ||  deal_group                         AS key_segment_geo_ot,      


        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region                                  AS key_segment_geo_region,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_geo || '_' ||  sales_qualified_source   AS key_segment_geo_region_sqs,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_geo || '_' ||  deal_group               AS key_segment_geo_region_ot,   

        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area                                       AS key_segment_geo_region_area,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' ||  sales_qualified_source     AS key_segment_geo_region_area_sqs,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' ||  deal_group                 AS key_segment_geo_region_area_ot,


        COALESCE(report_opportunity_user_segment ,'NA')                                    AS sales_team_cro_level,
     
        -- NF: This code replicates the reporting structured of FY22, to keep current tools working
        CASE 
          WHEN report_opportunity_user_segment = 'Large'
            AND report_opportunity_user_geo = 'EMEA'
              THEN 'Large_EMEA'
          WHEN report_opportunity_user_segment = 'Mid-Market'
            AND report_opportunity_user_region = 'AMER'
            AND lower(report_opportunity_user_area) LIKE '%west%'
              THEN 'Mid-Market_West'
          WHEN report_opportunity_user_segment = 'Mid-Market'
            AND report_opportunity_user_region = 'AMER'
            AND lower(report_opportunity_user_area) NOT LIKE '%west%'
              THEN 'Mid-Market_East'
          WHEN report_opportunity_user_segment = 'SMB'
            AND report_opportunity_user_region = 'AMER'
            AND lower(report_opportunity_user_area) LIKE '%west%'
              THEN 'SMB_West'
          WHEN report_opportunity_user_segment = 'SMB'
            AND report_opportunity_user_region = 'AMER'
            AND lower(report_opportunity_user_area) NOT LIKE '%west%'
              THEN 'SMB_East'
          ELSE COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_region),'NA') 
        END                                                                           AS sales_team_rd_asm_level,

        COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo),'NA')                                                                      AS sales_team_vp_level,
        COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo,'_',report_opportunity_user_region),'NA')                                   AS sales_team_avp_rd_level,
        COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo,'_',report_opportunity_user_region,'_',report_opportunity_user_area),'NA')  AS sales_team_asm_level

  FROM eligible
  WHERE LOWER(report_opportunity_user_segment) NOT IN ('jihu','other')
      AND LOWER(deal_group) != '3. other'
      AND LOWER(sales_qualified_source) NOT IN ('na','other')
  
 )
 
 SELECT *
 FROM valid_keys