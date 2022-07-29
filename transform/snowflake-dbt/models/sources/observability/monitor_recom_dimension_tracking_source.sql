WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'monitor_recom_dimension_tracking') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed