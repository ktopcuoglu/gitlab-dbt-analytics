{{config({
    "materialized": "table",
    "schema": "common"
  })
}}

WITH opportunity_source AS (

    SELECT
      dim_opportunity_source_id,
      opportunity_source_name
    FROM {{ ref('prep_opportunity_source') }}

)

{{ dbt_audit(
    cte_ref="opportunity_source",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-10-26",
    updated_date="2020-12-18"
) }}
