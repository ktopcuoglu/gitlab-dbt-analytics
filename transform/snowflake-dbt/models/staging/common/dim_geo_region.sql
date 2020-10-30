{{config({
    "materialized": "table",
    "schema": "common"
  })
}}

WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('[reference to region mapping table in common_mapping]') }}

), unioned AS (

  SELECT DISTINCT
    dim_geo_region_id AS dim_geo_region_id,
    geo_region_name   AS geo_region_name

  FROM sfdc_account

  UNION ALL

  SELECT
    '-1'                AS dim_geo_region_id,
    '(Missing Region)'  AS geo_region_name
)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@msendal",
    updated_by="@msendal",
    created_date="2020-10-30",
    updated_date="2020-10-30"
) }}
