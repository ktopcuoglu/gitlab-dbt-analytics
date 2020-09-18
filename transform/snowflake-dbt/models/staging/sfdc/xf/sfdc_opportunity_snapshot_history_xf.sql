/*

2020-09-15
It incorporates flags added to support the pipeline velocity report of @fkurniadi 
and the forecasting model for Commercial of Sales Strategy

*/ 
WITH RECURSIVE date_details AS (

    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), sfdc_opportunity_snapshot_history AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history') }}

), sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}

), sfdc_users_xf AS (

    SELECT * 
    FROM {{ref('sfdc_users_xf')}}

),  managers AS (
  SELECT
        id,
        name,
        role_name,
        manager_name,
        manager_id,
        0 AS level,
        '' AS path
  FROM sfdc_users_xf
  WHERE role_name = 'CRO'

UNION ALL

SELECT
      users.id,
      users.name,
      users.role_name,
      users.manager_name,
      users.manager_id,
      level + 1,
      path || managers.role_name || '::'
FROM sfdc_users_xf users
INNER JOIN managers
ON users.manager_id = managers.id

), cro_sfdc_hierarchy AS (

SELECT
      id,
      name,
      role_name,
      manager_name,
      SPLIT_PART(path, '::', 1)::VARCHAR(50) AS level_1,
      SPLIT_PART(path, '::', 2)::VARCHAR(50) AS level_2,
      SPLIT_PART(path, '::', 3)::VARCHAR(50) AS level_3,
      SPLIT_PART(path, '::', 4)::VARCHAR(50) AS level_4,
      SPLIT_PART(path, '::', 5)::VARCHAR(50) AS level_5
FROM managers

), sales_admin_bookings_hierarchy AS (
SELECT
        sfdc_opportunity_xf.opportunity_id,
        sfdc_opportunity_xf.owner_id,
        'CRO'                                                           AS level_1,
        CASE account_owner_team_stamped
            WHEN 'APAC'                 THEN 'VP Ent'
            WHEN 'Commercial'           THEN 'VP Comm SMB'
            WHEN 'Commercial - MM'      THEN 'VP Comm MM'
            WHEN 'Commercial - SMB'     THEN 'VP Comm SMB'
            WHEN 'EMEA'                 THEN 'VP Ent'
            WHEN 'MM - APAC'            THEN 'VP Comm MM'
            WHEN 'MM - East'            THEN 'VP Comm MM'
            WHEN 'MM - EMEA'            THEN 'VP Comm MM'
            WHEN 'MM - West'            THEN 'VP Comm MM'
            WHEN 'MM-EMEA'              THEN 'VP Comm MM'
            WHEN 'Public Sector'        THEN 'VP Ent'
            WHEN 'SMB'                  THEN 'VP Comm SMB'
            WHEN 'SMB - International'  THEN 'VP Comm SMB'
            WHEN 'SMB - US'             THEN 'VP Comm SMB'
            WHEN 'US East'              THEN 'VP Ent'
            WHEN 'US West'              THEN 'VP Ent'
            ELSE NULL END                                                  AS level_2,
        CASE account_owner_team_stamped
            WHEN 'APAC'                 THEN 'RD APAC'
            WHEN 'EMEA'                 THEN 'RD EMEA'
            WHEN 'MM - APAC'            THEN 'ASM - MM - APAC'
            WHEN 'MM - East'            THEN 'ASM - MM - East'
            WHEN 'MM - EMEA'            THEN 'ASM - MM - EMEA'
            WHEN 'MM - West'            THEN 'ASM - MM - West'
            WHEN 'MM-EMEA'              THEN 'ASM - MM - EMEA'
            WHEN 'Public Sector'        THEN 'RD PubSec'
            WHEN 'US East'              THEN 'RD US East'
            WHEN 'US West'              THEN 'RD US West'
            ELSE NULL END                                                   AS level_3
    FROM sfdc_opportunity_xf
    -- sfdc Sales Admin user
    WHERE owner_id = '00561000000mpHTAAY'

), final AS (

    SELECT h.date_actual                                                                                AS snapshot_date,

       --snapshot date helpers
        ds.first_day_of_month                                                                           AS snapshot_month,
        ds.fiscal_year                                                                                  AS snapshot_fiscal_year,
        ds.fiscal_quarter_name_fy                                                                       AS snapshot_fiscal_quarter,
        ds.first_day_of_fiscal_quarter                                                                  AS snapshot_fiscal_quarter_date,

       --close date helpers
        d.first_day_of_month                                                                            AS close_month,
        d.fiscal_year                                                                                   AS close_fiscal_year,
        d.fiscal_quarter_name_fy                                                                        AS close_fiscal_quarter,
        d.first_day_of_fiscal_quarter                                                                   AS close_fiscal_quarter_date,
        h.forecast_category_name                                                                        AS forecast_category_name,                  
        h.opportunity_id,
        h.owner_id                                                                                      AS owner_id,
        o.opportunity_owner_manager,                     
        h.stage_name,
        h.sales_type,
        h.is_deleted,
        a.tsp_region,
        a.tsp_sub_region,
        CASE WHEN sa.level_2 IS NOT NULL 
                THEN sa.level_2
                ELSE cro.level_2 END                                                                    AS segment,
        
        -- identify VP level managers
        CASE WHEN cro.level_2 LIKE 'VP%' 
            OR sa.level_2 LIKE 'VP%'
                THEN 1 ELSE 0 END                                                                       AS is_lvl_2_vp_flag,

        CASE WHEN o.order_type IS NULL THEN '3. Growth'
            ELSE o.order_type END                                                                       AS order_type, 
        CASE
            WHEN (a.sales_segment = 'Unknown' OR a.sales_segment IS NULL) 
                AND o.user_segment = 'SMB' 
                    THEN 'SMB'
            WHEN (a.sales_segment = 'Unknown' OR a.sales_segment IS NULL) 
                AND o.user_segment = 'Mid-Market' 
                    THEN 'Mid-Market'
            WHEN (a.sales_segment = 'Unknown' OR a.sales_segment IS NULL) 
                AND o.user_segment IN ('Large', 'US West', 'US East', 'Public Sector''EMEA', 'APAC') 
                    THEN 'Large'
            ELSE a.sales_segment END                                                                    AS sales_segment,
        CASE WHEN h.stage_name IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing', '1-Discovery', '2-Developing', '2-Scoping')  
                THEN 'Pipeline'
             WHEN h.stage_name IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                         
                THEN '3+ Pipeline'
             WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                       
                THEN 'Lost'
             WHEN h.stage_name IN ('Closed Won')                                                                                                         
                THEN 'Closed Won'
             ELSE 'Other' END                                                                           AS stage_name_3plus,
        CASE WHEN h.stage_name IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing','1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     
                THEN 'Pipeline'
            WHEN h.stage_name IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                                               
                THEN '4+ Pipeline'
            WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                                                   
                THEN 'Lost'
            WHEN h.stage_name IN ('Closed Won')                                                                                                                                     
                THEN 'Closed Won'
            ELSE 'Other' END                                                                            AS stage_name_4plus,
        -- excluded accounts 
        CASE WHEN a.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
                AND h.close_date < '2020-08-01' THEN 1 ELSE 0 END                                       AS is_excluded_flag,

        -- metrics
        h.forecasted_iacv,
        h.renewal_acv,
        h.total_contract_value,

        CASE WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost') 
                AND h.sales_type = 'Renewal'      
                    THEN h.renewal_acv*-1
            WHEN h.stage_name IN ('Closed Won')                                                     
                THEN h.forecasted_iacv  
            ELSE 0 END                                                                                  AS net_iacv,
        CASE WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost') 
                AND h.sales_type = 'Renewal'      
                    THEN h.renewal_acv*-1
            WHEN h.stage_name IN ('Closed Won') AND h.forecasted_iacv < 0                           
                    THEN h.forecasted_iacv
            ELSE 0 END                                                                                  AS churn_only

    FROM sfdc_opportunity_snapshot_history h
    -- close date
    INNER JOIN date_details d
        ON cast(h.close_date as date) = d.date_actual
    -- snapshot date
    INNER JOIN date_details ds
        ON h.date_actual = ds.date_actual
    -- current opportunity
    LEFT JOIN sfdc_opportunity_xf o     
        ON o.opportunity_id = h.opportunity_id
    -- accounts
    LEFT JOIN sfdc_accounts_xf a
        ON h.account_id = a.account_id 
    -- owner hierarchy
    LEFT JOIN cro_sfdc_hierarchy cro
        ON h.owner_id = cro.id
    -- sales admin hierarchy
    LEFT JOIN sales_admin_bookings_hierarchy sa
        ON h.opportunity_id = sa.opportunity_id
) 
SELECT *
FROM final