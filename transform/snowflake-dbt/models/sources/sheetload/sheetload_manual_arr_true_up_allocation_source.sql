WITH source AS (

    SELECT * 
    FROM {{ source('sheetload','manual_arr_true_up_allocation') }}

)
  
SELECT * 
FROM source

