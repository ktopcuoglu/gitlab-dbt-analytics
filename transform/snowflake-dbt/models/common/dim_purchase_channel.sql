{{config({
    "materialized": "table",
  })
}}

WITH purchase_channel AS (

    SELECT
      dim_purchase_channel_id,
      purchase_channel_name
    FROM {{ ref('prep_purchase_channel' )}}
)

{{ dbt_audit(
    cte_ref="purchase_channel",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2020-12-18"
) }}
