WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_type') }}

), renamed AS (

    SELECT

      id                    AS id,
      name                  AS name,
      description           AS description

    FROM source

)

SELECT *
FROM renamed
