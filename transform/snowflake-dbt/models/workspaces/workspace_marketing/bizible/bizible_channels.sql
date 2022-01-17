WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_channels_source') }}

)

SELECT *
FROM source