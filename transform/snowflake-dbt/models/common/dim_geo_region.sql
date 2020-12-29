WITH geo_region AS (

    SELECT
      dim_geo_region_id,
      geo_region_name
    FROM {{ ref('prep_geo_region') }}
)

{{ dbt_audit(
    cte_ref="geo_region",
    created_by="@msendal",
    updated_by="@mcooperDD",
    created_date="2020-11-04",
    updated_date="2020-12-18"
) }}
