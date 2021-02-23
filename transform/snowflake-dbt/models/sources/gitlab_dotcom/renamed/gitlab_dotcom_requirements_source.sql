WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_requirements_dedupe_source') }}
  
), renamed AS (

    SELECT
      id::NUMBER                                 AS requirement_id,
      created_at::TIMESTAMP                       AS created_at,
      updated_at::TIMESTAMP                       AS updated_at,
      project_id::NUMBER                         AS project_id,
      author_id::NUMBER                          AS author_id,
      iid::NUMBER                                AS requirement_iid,
      state::VARCHAR                              AS requirement_state
    FROM source

)

SELECT *
FROM renamed
