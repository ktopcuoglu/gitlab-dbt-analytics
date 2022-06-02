WITH source AS (

  SELECT 
    --Primary Key
      {{ dbt_utils.surrogate_key([authors]) }} AS dim_author_key,    
      authors
  FROM {{ ref('sheetload_books') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2022-06-02",
    updated_date="2022-06-02"
) }}