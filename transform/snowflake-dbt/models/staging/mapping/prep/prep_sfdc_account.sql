{{ config({
        "materialized": "view",
        "schema": "common_mapping"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE NOT is_deleted

), final AS (

    SELECT
      account_id                                                                                                          AS crm_account_id,
      TRIM(SPLIT_PART(tsp_sub_region, '-', 1))                                                                            AS tsp_sub_region_clean,
      TRIM(SPLIT_PART(tsp_region, '-', 1))                                                                                AS tsp_region_clean,
      TRIM(SPLIT_PART(REPLACE(tsp_area,'Mid - Atlantic', 'Mid Atlantic'), '-', 1))                                        AS tsp_area_clean,
      TRIM(tsp_territory)                                                                                                 AS tsp_territory_clean,
      TRIM(SPLIT_PART(df_industry, '-', 1))                                                                               AS df_industry_clean,
      CASE
      WHEN ultimate_parent_sales_segment IS NOT NULL
        AND ultimate_parent_sales_segment != 'Unknown'  THEN ultimate_parent_sales_segment
      WHEN ultimate_parent_sales_segment = 'Unknown'    THEN 'SMB'
      WHEN ultimate_parent_sales_segment IS NULL
        AND sales_segment != 'Unknown'                  THEN sales_segment
      WHEN ultimate_parent_sales_segment IS NULL
        AND sales_segment = 'Unknown'
        OR sales_segment IS NULL                        THEN 'SMB'
      END                                                                                                                 AS ultimate_parent_sales_segment_clean,
      MAX(tsp_area_clean) OVER (Partition by UPPER(TRIM(tsp_area_clean)))                                                 AS dim_geo_area_name_source,
      MAX(tsp_region_clean) OVER (Partition by UPPER(TRIM(tsp_region_clean)))                                             AS dim_geo_region_name_source,
      MAX(tsp_sub_region_clean) OVER (Partition by UPPER(TRIM(tsp_sub_region_clean)))                                     AS dim_geo_sub_region_name_source,
      MAX(tsp_territory_clean) OVER (Partition by UPPER(TRIM(tsp_territory_clean)))                                       AS dim_sales_territory_name_source,
      MAX(df_industry_clean) OVER (Partition by UPPER(TRIM(df_industry_clean)))                                           AS dim_industry_name_source,
      MAX(ultimate_parent_sales_segment_clean ) OVER (Partition by UPPER(TRIM(ultimate_parent_sales_segment_clean )))     AS dim_sales_segment_name_source
    FROM source
)



{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-10-30",
    updated_date="2020-12-10"
) }}
