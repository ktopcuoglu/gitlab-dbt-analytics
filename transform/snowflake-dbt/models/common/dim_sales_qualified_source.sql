
WITH sales_qualified_source AS (

    SELECT
      dim_sales_qualified_source_id,
      sales_qualified_source_name,
      sales_qualified_source_grouped
    FROM {{ ref('prep_sales_qualified_source') }}

)

{{ dbt_audit(
    cte_ref="sales_qualified_source",
    created_by="@paul_armstrong",
    updated_by="@jpeguero",
    created_date="2020-10-26",
    updated_date="2021-04-26"
) }}
