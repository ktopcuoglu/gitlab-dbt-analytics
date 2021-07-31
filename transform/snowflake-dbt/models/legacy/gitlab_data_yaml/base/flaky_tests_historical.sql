WITH source AS (

    SELECT *
    FROM {{ ref('flaky_tests_source') }}

), filtered AS (

    SELECT *
    FROM source
    WHERE rank = 1

)

SELECT *
FROM filtered
