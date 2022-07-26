WITH source AS (

    SELECT *
    FROM {{ ref('rca_query_change_source') }}

)

SELECT *
FROM source
