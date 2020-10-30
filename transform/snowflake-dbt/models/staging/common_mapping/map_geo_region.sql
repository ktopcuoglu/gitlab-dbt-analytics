--Create mapping table for clean-up in ANALYTICS.COMMON_MAPPING

WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE tsp_region IS NOT NULL
      AND NOT is_deleted

), mapping AS (

    SELECT DISTINCT
      tsp_region 		                                    AS tsp_region,
      UPPER(TRIM(tsp_region)) 					            AS tsp_region_dedup,
      {{ dbt_utils.surrogate_key(['tsp_region_dedup']) }} 	AS dim_geo_region_id,
      MAX(tsp_region) OVER (PARTITION BY tsp_region_dedup) 	AS geo_region_name

    FROM sfdc_account
)

{{ dbt_audit(
    cte_ref="mapping",
    created_by="@msendal",
    updated_by="@msendal",
    created_date="2020-10-30",
    updated_date="2020-10-30"
) }}
