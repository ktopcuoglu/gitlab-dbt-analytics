WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_details_dedupe_source') }}

), renamed AS (

    SELECT
      user_id::NUMBER                     AS user_id,
      job_title::VARCHAR                  AS job_title,
      other_role::VARCHAR                 AS other_role
    FROM source
    
)

SELECT  *
FROM renamed
