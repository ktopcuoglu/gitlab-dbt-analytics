WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'experiment_subjects') }}

), renamed AS (

    SELECT
      id::NUMBER                AS experiment_subject_id,
      experiment_id::NUMBER     AS experiment_id,
      user_id::NUMBER           AS user_id,
      group_id::NUMBER          AS group_id,
      project_id::NUMBER        AS project_id,
      variant::NUMBER           AS experiment_variant,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at,
      converted_at::TIMESTAMP   AS converted_at
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1    

)

SELECT *
FROM renamed
ORDER BY created_at