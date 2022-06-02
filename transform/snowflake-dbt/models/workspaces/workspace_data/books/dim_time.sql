WITH source AS (

  SELECT 
    --Primary Key
      {{ dbt_utils.surrogate_key([publication_date]) }} AS dim_time_key,    
      date_part(day, publication_date::date) AS day,
      date_part(month, publication_date::date) AS month, 
      date_part(year, publication_date::date) AS year
  FROM {{ ref('sheetload_books') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2022-06-02",
    updated_date="2022-06-02"
) }}