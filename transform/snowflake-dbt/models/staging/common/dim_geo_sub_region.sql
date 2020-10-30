{{config({
    "materialized": "table",
    "schema": "common"
  })
}}

WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('map_geo_region') }}

), unioned AS (

    SELECT DISTINCT
      dim_geo_sub_region_id   AS dim_geo_sub_region_id,
      geo_sub_region_name     AS geo_sub_region_name
    FROM sfdc_account

    UNION ALL

    SELECT
      '-1'                     AS dim_geo_sub_region_id,
      '(Missing Sub Region)'   AS geo_sub_region_name
)

{{ dbt_audit(
    cte_ref="mapping",
    created_by="@msendal",
    updated_by="@msendal",
    created_date="2020-10-30",
    updated_date="2020-10-30"
) }}
