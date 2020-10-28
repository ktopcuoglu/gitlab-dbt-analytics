{{config({
    "materialized": "table",
    "schema": "common"
  })
}}

WITH map_area AS (

    SELECT *
    FROM {{ ref('dim_sales_territory_map_area') }}

), unioned AS (

    SELECT DISTINCT
        dim_sales_territory_id,
        dim_sales_territory
    UNION ALL
    SELECT
        MD5('-1')           AS dim_sales_territory_id,
        '(Missing Area)'    AS dim_sales_territory

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-10-26",
    updated_date="2020-10-26"
) }}
