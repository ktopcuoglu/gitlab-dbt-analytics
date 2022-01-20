WITH source AS (

    SELECT *
    FROM {{ ref('bizible_touchpoints_source') }}

)

SELECT *
FROM source