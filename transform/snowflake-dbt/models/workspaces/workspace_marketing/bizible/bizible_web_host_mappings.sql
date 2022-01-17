WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_web_host_mappings_source') }}

)

SELECT *
FROM source