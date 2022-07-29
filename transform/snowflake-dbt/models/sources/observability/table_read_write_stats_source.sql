WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'table_read_write_stats') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed