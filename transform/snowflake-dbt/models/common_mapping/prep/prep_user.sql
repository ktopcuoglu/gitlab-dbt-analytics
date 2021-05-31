-- very minimal prep_user model in order to get dim_event table

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER                                                       AS dim_user_id,
      remember_created_at::TIMESTAMP                                   AS remember_created_at,
      sign_in_count::NUMBER                                            AS sign_in_count,
      current_sign_in_at::TIMESTAMP                                    AS current_sign_in_at,
      last_sign_in_at::TIMESTAMP                                       AS last_sign_in_at,
      -- current_sign_in_ip   // hidden for privacy
      -- last_sign_in_ip   // hidden for privacy
      created_at::TIMESTAMP                                            AS created_at,
      updated_at::TIMESTAMP                                            AS updated_at,
      admin::BOOLEAN                                                   AS is_admin

    FROM source
    
)

SELECT  *
FROM renamed
ORDER BY updated_at
