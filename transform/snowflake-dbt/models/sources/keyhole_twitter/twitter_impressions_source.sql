WITH source AS (

    SELECT *
    FROM {{ source('keyhole_twitter', 'impressions') }}

), renamed AS (

    SELECT
      field::TIMESTAMP                      AS impression_month,
      value::INT                            AS impressions,
      _updated_at::FLOAT                    AS updated_at
    FROM source

)

SELECT *
FROM renamed
