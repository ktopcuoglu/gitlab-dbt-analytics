WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'top_queries_per_executor') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed