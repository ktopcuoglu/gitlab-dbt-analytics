WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_account_source') }}
  WHERE is_deleted = FALSE

)

SELECT
  TRIM(SPLIT_PART(tsp_sub_region, '-', 1))                                            AS tsp_sub_region_clean,
  TRIM(SPLIT_PART(tsp_region, '-', 1))                                                AS tsp_region_clean,
  TRIM(SPLIT_PART(REPLACE(tsp_area,'Mid - Atlantic', 'Mid Atlantic'), '-', 1))        AS tsp_area_clean,
  TRIM(SPLIT_PART(tsp_territory, '-1', 1))                                            AS tsp_territory_clean,
  account_id                                                                          AS crm_account_id,
  MAX(tsp_area_clean) OVER (Partition by UPPER(TRIM(tsp_area_clean)))                 AS dim_area_name_source,
  MAX(tsp_region_clean) OVER (Partition by UPPER(TRIM(tsp_region_clean)))             AS dim_region_name_source,
  MAX(tsp_sub_region_clean) OVER (Partition by UPPER(TRIM(tsp_sub_region_clean)))     AS dim_sub_region_name_source,
  MAX(tsp_territory_clean) OVER (Partition by UPPER(TRIM(tsp_territory_clean)))       AS dim_sales_territory_source
FROM source
