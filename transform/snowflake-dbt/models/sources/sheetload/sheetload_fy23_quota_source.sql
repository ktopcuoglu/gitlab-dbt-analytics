WITH source AS (

  SELECT * 
  FROM {{ source('sheetload','fy23_quota') }}

        )
SELECT * 
FROM source
