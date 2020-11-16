WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_targets_kpi_totals_source') }}

), final AS (

    SELECT
      kpi_name,
      fiscal_year_target,
      is_additive,
      formula,
      priority
    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-11-13",
    updated_date="2020-11-13"
) }}

