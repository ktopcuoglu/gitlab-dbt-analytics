WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'namespace_onboarding_actions') }}

), renamed AS (

    SELECT
      id::NUMBER                AS namespace_onboarding_action_id,
      namespace_id::NUMBER      AS namespace_id,
      created_at::TIMESTAMP     AS created_at,
      action::NUMBER            AS action_id
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1    

)

SELECT *
FROM renamed
ORDER BY created_at