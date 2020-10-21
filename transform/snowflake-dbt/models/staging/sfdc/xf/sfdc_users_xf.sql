WITH RECURSIVE users AS (

    SELECT *
    FROM {{ref('sfdc_users')}}

), user_role AS (

    SELECT *
    FROM {{ref('sfdc_user_roles')}}

), base AS (

    SELECT
      users.name            AS name,
      users.department      AS department,
      users.title           AS title,
      users.team,           --team,
      users.user_id,        --user_id
      manager.name          AS manager_name,
      manager.user_id       AS manager_id,
      user_role.name        AS role_name,
      users.start_date,      --start_date
      users.is_active        --is_active
    FROM users
    LEFT OUTER JOIN user_role 
      ON users.user_role_id = user_role.id
    LEFT OUTER JOIN users AS manager 
      ON manager.user_id = users.manager_id

),  managers AS (
    
    SELECT
      user_id,
      name,
      role_name,
      manager_name,
      manager_id,
      0 AS level,
      '' AS path
    FROM base
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
    FROM base users
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

)

SELECT 
  base.*,

  -- account owner hierarchies levels
  TRIM(cro.level_2)                                                                               AS sales_team_level_2,
  TRIM(cro.level_3)                                                                               AS sales_team_level_3,
  TRIM(cro.level_4)                                                                               AS sales_team_level_4,
  CASE 
    WHEN TRIM(cro.level_2) IS NOT NULL
      THEN TRIM(cro.level_2)
    ELSE 'n/a'
  END                                                                                             AS sales_team_vp_level,
  CASE 
    WHEN (TRIM(cro.level_3) IS NOT NULL  AND TRIM(cro.level_3) != '')
      THEN TRIM(cro.level_3)
    ELSE 'n/a'
  END                                                                                             AS sales_team_rd_level,
  CASE 
    WHEN cro.level_3 LIKE 'ASM%' 
      THEN cro.level_3
    WHEN cro.level_4 LIKE 'ASM%' OR cro.level_4 LIKE 'Area Sales%' 
      THEN cro.level_4
      ELSE 'n/a'
  END                                                                                             AS sales_team_asm_level,
  CASE 
    WHEN (cro.level_4 IS NOT NULL 
      AND cro.level_4 != ''
      AND (cro.level_4 LIKE 'ASM%' OR cro.level_4 LIKE 'Area Sales%') )
        THEN cro.level_4 
    WHEN (cro.level_3 IS NOT NULL AND cro.level_3 != '')
      THEN cro.level_3
    WHEN (cro.level_2 IS NOT NULL AND cro.level_2 != '')
      THEN cro.level_2
    ELSE 'n/a'
  END                                                                                             AS sales_min_hierarchy_level,
    CASE sales_min_hierarchy_level
      WHEN 'ASM - APAC - Japan'                 THEN 'APAC'
      WHEN 'ASM - Civilian'                     THEN 'PUBSEC'
      WHEN 'ASM - DoD - USAF+COCOMS+4th Estate' THEN 'PUBSEC'
      WHEN 'ASM - EMEA - DACH'                  THEN 'EMEA'
      WHEN 'ASM - EMEA - North'                 THEN 'EMEA'
      WHEN 'ASM - MM - EMEA'                    THEN 'EMEA'
      WHEN 'ASM - MM - East'                    THEN 'US East'
      WHEN 'ASM - MM - West'                    THEN 'US West'
      WHEN 'ASM - NSG'                          THEN 'PUBSEC'
      WHEN 'ASM - SLED'                         THEN 'PUBSEC'
      WHEN 'ASM - US East - Southeast'          THEN 'US East'
      WHEN 'ASM - US West - NorCal'             THEN 'US West'
      WHEN 'ASM - US West - PacNW'              THEN 'US West'
      WHEN 'ASM - US West - SoCal+Rockies'      THEN 'US West'
      WHEN 'ASM-DOD- Army+Navy+Marines+SI''s'   THEN 'PUBSEC'
      WHEN 'ASM-SMB-AMER-East'                  THEN 'US East'
      WHEN 'ASM-SMB-AMER-West'                  THEN 'US West'
      WHEN 'ASM-SMB-EMEA'                       THEN 'EMEA'
      WHEN 'Area Sales Manager - US East - Central'           THEN 'US East'
      WHEN 'Area Sales Manager - US East - Named Accounts'    THEN 'US East'
      WHEN 'Area Sales Manager - US East - Northeast'         THEN 'US East'
      WHEN 'CD EMEA'                            THEN 'EMEA'
      WHEN 'CD PubSec'                          THEN 'PUBSEC'
      WHEN 'RD APAC'                            THEN 'APAC'
      WHEN 'RD EMEA'                            THEN 'EMEA'
      WHEN 'RD PubSec'                          THEN 'PUBSEC'
      WHEN 'RD US East'                         THEN 'US East'
      WHEN 'RD US West'                         THEN 'US West'
      WHEN 'VP Comm MM'                         THEN 'Other'
      WHEN 'VP Ent'                             THEN 'Other'
      ELSE 'n/a'
    END                                                                                           AS sales_region,
    -- identify VP level managers
    CASE 
      WHEN cro.level_2 LIKE 'VP%' 
        THEN 1
      ELSE 0
    END                                                                                           AS is_lvl_2_vp_flag
FROM base
LEFT JOIN cro_sfdc_hierarchy cro
    ON cro.user_id = base.user_id
