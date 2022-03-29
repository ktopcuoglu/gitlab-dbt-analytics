WITH source AS (

    SELECT *
    FROM {{ ref('hp_dvr_source') }}

), renamed AS (

    SELECT
      date,
      region,
      country,
      name,
      numberrange,
      alphanumeric
    FROM source

)

SELECT *
FROM renamed