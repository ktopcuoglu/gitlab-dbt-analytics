
-- very minimal prep_user model in order to get dim_event table

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_source') }}

), renamed AS (

    SELECT
      user_id                                                          AS dim_user_id,
      remember_created_at                                              AS remember_created_at,
      sign_in_count                                                    AS sign_in_count,
      current_sign_in_at                                               AS current_sign_in_at,
      last_sign_in_at                                                  AS last_sign_in_at,
      created_at                                                       AS created_at,
      updated_at                                                       AS updated_at,
      is_admin                                                         AS is_admin,
      state                                                            AS user_state,
      CASE 
        WHEN state in ('blocked', 'banned') THEN TRUE
        ELSE FALSE 
      END                                                              AS is_blocked_user
    FROM source
    
)

SELECT  *
FROM renamed
ORDER BY updated_at
