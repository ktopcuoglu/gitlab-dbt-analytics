WITH source AS (

    SELECT *
    FROM {{ source('keyhole_twitter', 'impressions') }}

), renamed AS (

    SELECT
      FIELD::timestamp                      AS impression_month,
      VALUE::int                            AS impressions,
      _UPDATED_AT::FLOAT                    AS updated_at
    FROM source

)

SELECT *
FROM renamed
