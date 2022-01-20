WITH source AS (

    SELECT *
    FROM {{ ref('bizible_opportunities_source') }}

)

SELECT *
FROM source