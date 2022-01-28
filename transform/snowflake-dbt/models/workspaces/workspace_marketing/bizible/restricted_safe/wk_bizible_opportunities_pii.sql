WITH source AS (

    SELECT *
    FROM {{ ref('bizible_opportunities_source_pii') }}

)

SELECT *
FROM source