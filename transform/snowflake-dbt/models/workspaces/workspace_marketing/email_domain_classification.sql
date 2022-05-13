WITH base AS (

    SELECT *
    FROM {{ ref('driveload_email_domain_classification_source') }}

)

SELECT *
FROM base
