WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_accounts_source') }}

)

SELECT *
FROM source