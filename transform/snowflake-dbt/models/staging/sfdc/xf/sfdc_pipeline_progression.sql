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

)
SELECT h.date_actual                                                    AS snapshot_date,
    d.first_day_of_month                                                AS close_month,
    d.fiscal_year                                                       AS close_fiscal_year,
    h.forecast_category_name                                            AS snapshot_forecast_category_name,                  
    h.opportunity_id,
    h.owner_id                                                          AS snapshot_owner_id,
    a.tsp_region,
    a.tsp_sub_region,
    h.stage_name                                                        AS snapshot_stage_name,
    CASE WHEN sales_admin_bookings_hierarchy.level_2 IS NOT NULL 
            THEN sales_admin_bookings_hierarchy.level_2
            ELSE cro_sfdc_hierarchy.level_2 END                         AS segment,
    COUNT(DISTINCT h.opportunity_id)                                    AS opps,
    SUM(CASE
                WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost') 
                    AND h.sales_type = 'Renewal'      
                    THEN h.renewal_acv*-1
                WHEN h.stage_name IN ('Closed Won')                                                     
                    THEN h.forecasted_iacv
                ELSE 0 END )                                            AS net_iacv,
    -- why closed/lost counts as pipeline?
    SUM(h.forecasted_iacv)                                              AS forecasted_iacv
FROM sfdc_opportunity_snapshot_history h
-- accounts
LEFT JOIN sfdc_accounts_xf a
    ON h.account_id = a.account_id 
-- close date
INNER JOIN date_details d
    ON h.close_date = d.date_actual
-- owner hierarchy
LEFT JOIN cro_sfdc_hierarchy
    ON h.owner_id = cro_sfdc_hierarchy.id
-- sales admin hierarchy
LEFT JOIN sales_admin_bookings_hierarchy
    ON h.opportunity_id = sales_admin_bookings_hierarchy.opportunity_id
-- remove lost & deleted deals
WHERE h.stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified')
    AND (cro_sfdc_hierarchy.level_2 LIKE 'VP%' 
        OR sales_admin_bookings_hierarchy.level_2 LIKE 'VP%')
    AND h.date_actual >= dateadd(month,-1, d.first_day_of_month)
    AND h.date_actual <= dateadd(month, 1, d.first_day_of_month)
    AND h.is_deleted = 0
GROUP BY 1,2,3,4,5,6,7,8,9,10
