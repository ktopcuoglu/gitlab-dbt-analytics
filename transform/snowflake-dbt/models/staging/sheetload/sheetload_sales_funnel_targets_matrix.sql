WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source') }}

), final AS (

    SELECT
      kpi_name,
      month,
      sales_segment,
      opportunity_source,
      order_type,
      region,
      area,
      allocated_target,
      kpi_total
    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2020-11-18",
    updated_date="2020-11-18"
) }}
