WITH source AS (

  SELECT 
    --Primary Key
      {{ dbt_utils.surrogate_key([bookID, isbn13]) }} AS dim_book_id,    
      title AS book_title, 
      language,
      isbn, 
      num_pages
  FROM {{ ref('sheetload_books') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2022-06-02",
    updated_date="2022-06-02"
) }}