WITH source AS (

  SELECT 
    --Primary Key
      {{ dbt_utils.surrogate_key([bookID, isbn13]) }} AS dim_rating_key, 
    
    --Foreign Keys
      {{ dbt_utils.surrogate_key([publication_date]) }} AS dim_time_key,    
      {{ dbt_utils.surrogate_key([bookID, isbn13]) }} AS dim_book_id,    
      {{ dbt_utils.surrogate_key([authors]) }} AS dim_author_key,    
      {{ dbt_utils.surrogate_key([publisher]) }} AS dim_publisher_key, 

    
    --Measures
      text_reviews_count, 
      average_rating, 
      ratings_count
  FROM {{ ref('sheetload_books') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@michellecooper",
    updated_by="@lisvinueza",
    created_date="2022-06-01",
    updated_date="2022-06-02"
) }}