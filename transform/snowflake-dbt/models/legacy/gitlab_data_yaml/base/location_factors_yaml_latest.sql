WITH source AS (

    SELECT *
    FROM {{ ref('location_factors_yaml_historical') }}

), max_date AS (

    SELECT *
    FROM source
    WHERE valid_to_date = (SELECT max(valid_to_date) FROM source)

)

SELECT *
FROM max_date