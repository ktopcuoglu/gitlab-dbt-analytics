WITH alliance_type AS (

    SELECT
      dim_alliance_type_id,
      alliance_type_name,
      alliance_type_short_name
    FROM {{ ref('prep_alliance_type') }}
)

{{ dbt_audit(
    cte_ref="alliance_type",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-04-07",
    updated_date="2021-04-07"
) }}
