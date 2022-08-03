WITH source AS (

    SELECT *
    FROM {{ ref('top_queries_per_executor_source') }}

)

SELECT *
FROM source
