WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_account_source') }}
  WHERE is_deleted = FALSE

)

SELECT
  TRIM(SPLIT_PART(tsp_sub_region, '-', 1))                                            AS tsp_sub_region_clean,
  TRIM(SPLIT_PART(tsp_region, '-', 1))                                                AS tsp_region_clean,
  TRIM(SPLIT_PART(REPLACE(tsp_area,'Mid - Atlantic', 'Mid Atlantic'), '-', 1))        AS tsp_area_clean,
  UPPER(TRIM(tsp_territory))                                                          AS tsp_territory,
  account_id                                                                          AS crm_account_id,
  MAX(tsp_area_clean) OVER (Partition by UPPER(TRIM(tsp_area_clean)))                 AS dim_area_name_source,
  MAX(tsp_region_clean) OVER (Partition by UPPER(TRIM(tsp_region_clean)))             AS dim_region_name_source,
  MAX(tsp_sub_region_clean) OVER (Partition by UPPER(TRIM(tsp_sub_region_clean)))     AS dim_sub_region_name_source
FROM source
GROUP BY account_id
