WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source') }}

), final AS (

    SELECT
      kpi_name,
      month,
      opportunity_source,
      order_type,
      area,
      allocated_target,
      kpi_total,
      month_percentage,
      opportunity_source_percentage,
      order_type_percentage,
      area_percentage
    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2020-11-18",
    updated_date="2020-11-18"
) }}
