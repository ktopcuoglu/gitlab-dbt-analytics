WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_ad_accounts_source') }}

)

SELECT *
FROM source