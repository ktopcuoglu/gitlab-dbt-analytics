WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_credit_card_validations_dedupe_source') }}

), renamed AS (

    SELECT
      user_id::NUMBER                       AS user_id,
      credit_card_validated_at::TIMESTAMP  AS credit_card_validated_at
    FROM source
    
)

SELECT  *
FROM renamed
