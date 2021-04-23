WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_in_product_marketing_emails_source') }}


)

SELECT *
FROM source