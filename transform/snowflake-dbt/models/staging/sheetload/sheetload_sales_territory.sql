WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_territory_source') }}

), final AS (

    SELECT
      kpi_name,
      sales_territory,
      target,
      percent_curve
    FROM source

)
{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-11-13",
    updated_date="2020-11-13"
) }}

