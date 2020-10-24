WITH source AS (

    SELECT *
    FROM {{ source('rspec', 'overall_time') }}

), renamed AS (

    SELECT
      FIELD::timestamp                      AS impression_month,
      VALUE::int                            AS impressions,
      _UPDATED_AT::FLOAT                    AS updated_at,
    FROM source

)

SELECT *
FROM renamed
