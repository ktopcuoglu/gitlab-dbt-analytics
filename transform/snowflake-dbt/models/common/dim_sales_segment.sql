WITH sales_segment AS (

    SELECT
      dim_sales_segment_id,
      sales_segment_name
    FROM {{ ref('prep_sales_segment') }}
)

{{ dbt_audit(
    cte_ref="sales_segment",
    created_by="@msendal",
    updated_by="@mcooperDD",
    created_date="2020-11-05",
    updated_date="2020-12-18"
) }}
