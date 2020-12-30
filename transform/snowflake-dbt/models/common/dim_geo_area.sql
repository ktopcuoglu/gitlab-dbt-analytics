WITH geo_area AS (

    SELECT
      dim_geo_area_id,
      geo_area_name
    FROM {{ ref('prep_geo_area') }}
)

{{ dbt_audit(
    cte_ref="geo_area",
    created_by="@msendal",
    updated_by="@mcooperDD",
    created_date="2020-11-04",
    updated_date="2020-12-18"
) }}
