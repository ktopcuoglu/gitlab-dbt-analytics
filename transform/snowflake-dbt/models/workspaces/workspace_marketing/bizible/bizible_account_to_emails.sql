WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_account_to_emails_source') }}
    
)

SELECT *
FROM source