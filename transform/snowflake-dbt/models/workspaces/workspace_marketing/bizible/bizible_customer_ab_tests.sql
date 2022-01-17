WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_customer_ab_tests_source') }}

)

SELECT *
FROM source