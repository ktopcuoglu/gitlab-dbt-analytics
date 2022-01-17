WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_facts_source') }}

)

SELECT *
FROM source