WITH source AS (

    SELECT *
    FROM {{ source('zuora_query_api', 'users') }}

), renamed AS (

    SELECT
      id
    FROM source

)

SELECT *
FROM renamed
