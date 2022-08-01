WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'rca_query_change') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed