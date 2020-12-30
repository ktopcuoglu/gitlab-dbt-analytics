

WITH order_type AS (

    SELECT
      dim_order_type_id,
      order_type_name
    FROM {{ ref('prep_order_type') }}
)

{{ dbt_audit(
    cte_ref="order_type",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-11-02",
    updated_date="2020-12-18"
) }}
