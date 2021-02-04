WITH source AS (

    SELECT *
    FROM {{ source('license', 'granted_add_ons') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at::TIMESTAMP DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER            AS granted_add_on_id,
      license_id::NUMBER    AS license_id,
      add_on_id::NUMBER     AS add_on_id,
      quantity::NUMBER      AS quantity,
      created_at::TIMESTAMP AS created_at,
      updated_at::TIMESTAMP AS updated_at
    FROM source

)

SELECT *
FROM renamed
