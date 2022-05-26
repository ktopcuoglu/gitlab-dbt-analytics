WITH source AS (

  SELECT * 
  FROM {{ source('sheetload','books') }}

)
SELECT * 
FROM source
