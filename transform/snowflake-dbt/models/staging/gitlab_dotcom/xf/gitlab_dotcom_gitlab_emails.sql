WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_emails_source') }}

), filtered AS (

    SELECT *
    FROM source
    WHERE LOWER(email_address) LIKE '%@gitlab.com'

)

SELECT *
FROM filtered
