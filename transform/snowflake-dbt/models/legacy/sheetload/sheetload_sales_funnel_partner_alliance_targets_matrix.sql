WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_partner_alliance_targets_matrix_source') }}

), final AS (

    SELECT
      kpi_name,
      month,
      partner_engagement_type,
      alliance_partner,
      order_type,
      area,
      allocated_target
    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-04-05",
    updated_date="2021-04-05"
) }}
