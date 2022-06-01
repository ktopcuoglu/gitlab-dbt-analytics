WITH source AS (

  SELECT *
  FROM {{ ref('sheetload_books') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-06-01",
    updated_date="2022-06-01"
) }}