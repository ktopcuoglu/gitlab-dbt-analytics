WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'email_domain_classification') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY domain ORDER BY _updated_at DESC) = 1

)

SELECT
  domain::VARCHAR               AS domain,
  classification::VARCHAR       AS classification
FROM source
