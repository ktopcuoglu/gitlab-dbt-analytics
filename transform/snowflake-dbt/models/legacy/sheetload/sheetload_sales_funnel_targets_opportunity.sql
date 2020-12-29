WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_targets_opportunity_source') }}

), final AS (

    SELECT
      fields_concatenated,
      kpi_name,
      opportunity_source,
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

