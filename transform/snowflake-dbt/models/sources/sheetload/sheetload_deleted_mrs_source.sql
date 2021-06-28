WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'deleted_mrs') }}

), renamed AS (

    SELECT 
      merge_request_id::INTEGER AS merge_request_id
    FROM source

)

SELECT * 
FROM renamed
