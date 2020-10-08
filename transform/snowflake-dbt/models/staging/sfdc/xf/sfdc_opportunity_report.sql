 WITH date_details AS (

    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), sfdc_users_xf AS (
    
    SELECT * 
    FROM {{ref('sfdc_users_xf')}}

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), sfdc_opportunity_xf AS (
    
    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}
    WHERE is_deleted = 0

),  managers AS (
    
    SELECT
        user_id,
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
        users.user_id,
        users.name,
        users.role_name,
        users.manager_name,
        users.manager_id,
        level + 1,
        path || managers.role_name || '::'
    FROM sfdc_users_xf users
        INNER JOIN managers
            ON users.manager_id = managers.user_id

), cro_sfdc_hierarchy AS (

    SELECT
        user_id,
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
)  
SELECT o.*,
        
        CASE WHEN o.order_type_stamped = '1. New - First Order' 
                THEN '1. New'
            WHEN o.order_type_stamped IN ('2. New - Connected', '3. Growth') 
                THEN '2. Growth' 
            WHEN o.order_type_stamped = '4. Churn'
                THEN '3. Churn'
            ELSE '4. Other' END                                                                         AS deal_category,
        
        CASE
            WHEN (o.ultimate_parent_sales_segment  = 'Unknown' OR o.ultimate_parent_sales_segment  IS NULL) 
                AND o.user_segment = 'SMB' 
                    THEN 'SMB'
            WHEN (o.ultimate_parent_sales_segment  = 'Unknown' OR o.ultimate_parent_sales_segment  IS NULL) 
                AND o.user_segment = 'Mid-Market' 
                    THEN 'Mid-Market'
            WHEN (o.ultimate_parent_sales_segment  = 'Unknown' OR o.ultimate_parent_sales_segment  IS NULL) 
                AND o.user_segment IN ('Large', 'US West', 'US East', 'Public Sector''EMEA', 'APAC') 
                    THEN 'Large'
            ELSE o.ultimate_parent_sales_segment END                                                    AS adj_sales_segment,
        
            
        CASE WHEN o.account_owner_team_stamped in ('APAC', 'MM - APAC')
                THEN 'APAC'
            WHEN o.account_owner_team_stamped in ('MM - EMEA', 'EMEA', 'MM-EMEA')
                THEN 'EMEA'
            WHEN o.account_owner_team_stamped in ('US East', 'MM - East')
                THEN 'US EAST'
            WHEN o.account_owner_team_stamped in ('US West', 'MM - West')
                THEN 'US WEST'
            WHEN o.account_owner_team_stamped in ('Public Sector')
                THEN 'PUBSEC'
            ELSE 'OTHER' END                                                                            AS adj_region,

        CASE WHEN sa.level_2 IS NOT NULL 
                THEN sa.level_2
                ELSE trim(cro.level_2) END                                                              AS team_level_2,

        CASE WHEN sa.level_3 IS NOT NULL 
                THEN sa.level_3
                ELSE trim(cro.level_3) END                                                              AS team_level_3,

        -- identify VP level managers
        CASE WHEN cro.level_2 LIKE 'VP%' 
            OR sa.level_2 LIKE 'VP%'
                THEN 1 ELSE 0 END                                                                       AS is_lvl_2_vp_flag,

        -- helper flags

        CASE WHEN o.stage_name 
                  IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing', '1-Discovery', '2-Developing', '2-Scoping')  THEN 'Pipeline'													
                  WHEN o.stage_name 
                  IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                               THEN '3+ Pipeline'													
                  WHEN o.stage_name 
                  IN ('8-Closed Lost', 'Closed Lost')                                                                                             THEN 'Lost'													  
                  WHEN o.stage_name IN ('Closed Won')                                                                                             THEN 'Closed Won'													
                  ELSE 'Other'													
                  END                                                                                   AS stage_name_3plus,												
        CASE WHEN o.stage_name 
                  IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing','1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     
                              THEN 'Pipeline'													
                  WHEN o.stage_name 
                  IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                           THEN '4+ Pipeline'													
                  WHEN o.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                             THEN 'Lost'													
                  WHEN o.stage_name IN ('Closed Won')                                                                                               THEN 'Closed Won'													
                  ELSE 'Other'													
                  END                                                                                   AS stage_name_4plus,	

        CASE WHEN o.stage_name 
                  IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               THEN 1												
    										
                  ELSE 0													
                  END                                                                                   AS is_stage_3_plus,

        CASE WHEN o.stage_name = '8-Closed Lost'  THEN 1 ELSE 0 END                                     AS is_lost,
        CASE WHEN (o.stage_name = '8-Closed Lost' 
              OR o.stage_name = '9-Unqualified'
              OR o.is_won = 1) THEN 0 ELSE 1 END                                                        AS is_open,

        CASE WHEN is_open = 0 THEN 1 ELSE 0 END                                                         AS is_closed,
        CASE WHEN o.is_won = 1 THEN '1.Won'
            WHEN is_lost = 1 THEN '2.Lost'
            WHEN is_open = 1 THEN '0. Open' 
            ELSE 'N/A' END                                                                              AS stage_category,
  
        CASE WHEN  lower(o.sales_type) like '%renewal%' 
          -- OR op.renewal_acv > 0
          THEN 1 ELSE 0 END                                                                             AS is_renewal, 

        -- date fields
        d.fiscal_quarter_name_fy                                                                        AS close_fiscal_quarter_name,
        d.first_day_of_fiscal_quarter                                                                   AS close_fiscal_quarter_date,
        d.fiscal_year                                                                                   AS close_fiscal_quarter_year,
        d.first_day_of_month                                                                            AS close_date_month,
        
        dc.fiscal_quarter_name_fy                                                                       AS created_fiscal_quarter_name,
        dc.first_day_of_fiscal_quarter                                                                  AS created_fiscal_quarter_date,
        dc.fiscal_year                                                                                  AS created_fiscal_year,
        dc.first_day_of_month                                                                           AS created_date_month,

        -- subscription start date extra fields
        start_date.fiscal_year                                                                          AS start_date_fiscal_year,
        start_date.fiscal_quarter_name_fy                                                               AS start_date_fiscal_quarter,
        start_date.first_day_of_month                                                                   AS start_date_month,
        -- sales accepted date
        dsa.fiscal_quarter_name_fy                                                                      AS sales_accepted_fiscal_quarter,
        dsa.fiscal_year                                                                                 AS sales_accepted_fiscal_year,
        dsa.first_day_of_month                                                                          AS sales_accepted_date_month,

        -- sales qualified date
        dqa.fiscal_quarter_name_fy                                                                      AS sales_qualified_fiscal_quarter,
        dqa.fiscal_year                                                                                 AS sales_qualified_fiscal_year,
        dqa.first_day_of_month                                                                          AS sales_qualified_date_month      

   FROM sfdc_opportunity_xf o
    -- close date
    INNER JOIN date_details d
        ON d.date_actual = cast(o.close_date as date)
    --created date
    INNER JOIN date_details dc
        ON dc.date_actual = cast(o.created_date as date)
    -- sales accepted date
    LEFT JOIN date_details dsa
        ON cast(o.sales_accepted_date as date) = dsa.date_actual
    -- subscription start date data
    LEFT JOIN date_details start_date
        ON cast(o.subscription_start_date as date) = start_date.date_actual
    -- sales qualified date
    LEFT JOIN date_details dqa
        ON CAST(o.sales_qualified_date as date) = dqa.date_actual
    -- owner hierarchy
    LEFT JOIN cro_sfdc_hierarchy cro
        ON o.owner_id = cro.user_id
    -- sales admin hierarchy
    LEFT JOIN sales_admin_bookings_hierarchy sa
        ON o.opportunity_id = sa.opportunity_id
