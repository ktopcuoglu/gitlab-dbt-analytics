WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_credit_card_validations_source') }}

)

SELECT *
FROM source
