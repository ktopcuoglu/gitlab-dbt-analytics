WITH source AS (

    SELECT *
    FROM {{ ref('bizible_web_host_mappings_source_pii') }}

)

SELECT *
FROM source