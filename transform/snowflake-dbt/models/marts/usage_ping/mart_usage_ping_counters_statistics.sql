WITH flattened_usage_data AS (

    SELECT *
    FROM {{ ref('version_usage_data') }},
      lateral flatten(input => version_usage_data.raw_usage_data_payload, recursive => True) f

), transformed AS (
  
    SELECT DISTINCT
      path                                                    AS ping_name, 
      IFF(edition='CE', edition, 'EE')                        AS edition,
      SPLIT_PART(ping_name, '.', 1)                           AS main_json_name,
      SPLIT_PART(ping_name, '.', -1)                          AS feature_name,
      FIRST_VALUE(major_minor_version) OVER (
        PARTITION BY ping_name 
        ORDER BY  major_version ASC, minor_version ASC
      )                                                       AS first_version_with_counter,
      MIN(major_version) OVER (
        PARTITION BY ping_name
      )                                                       AS firt_major_version_with_counter,
      FIRST_VALUE(minor_version) OVER (
        PARTITION BY ping_name 
        ORDER BY major_version ASC, minor_version ASC
      )                                                       AS firt_minor_version_with_counter,
      LAST_VALUE(major_minor_version) OVER (
        PARTITION BY ping_name 
        ORDER BY major_version ASC, minor_version ASC
      )                                                       AS last_version_with_counter,
      MAX(major_version) OVER (
        PARTITION BY ping_name
      )                                                       AS last_major_version_with_counter,
      LAST_VALUE(minor_version) OVER (
        PARTITION BY ping_name 
        ORDER BY major_version ASC, minor_version ASC
      )                                                       AS last_minor_version_with_counter,
      COUNT(DISTINCT id) OVER (PARTITION BY ping_name)        AS count_pings,
      COUNT(DISTINCT uuid) OVER (PARTITION BY ping_name)      AS count_instances
    FROM flattened_usage_data
    WHERE TRY_TO_DECIMAL(value::TEXT) > 0
      -- Removing SaaS
      AND uuid <> 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      -- Removing pre-releases
      AND version NOT LIKE '%pre'

)

SELECT *
FROM transformed
