
WITH source AS (

  SELECT *
  FROM {{ source('edcast', 'glue_groups_g3_group_performance_data_explorer') }}

), deduplicated AS (

  SELECT *
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ecl_id,
    time,
    group_name,
    card_resource_url,
    assigned_content,
    event,
    card_author_full_name,
    follower_user_full_name_,
    following_user_full_name_,
    user_full_name,
    __loaded_at ORDER BY __loaded_at DESC) = 1

), renamed AS (

  SELECT
    assigned_content::BOOLEAN                       AS assigned_content,
    NULLIF(card_author_full_name,'')::VARCHAR       AS card_author_full_name,
    NULLIF(card_resource_url,'')::VARCHAR           AS card_resource_url,
    NULLIF(card_state,'')::VARCHAR                  AS card_state,
    NULLIF(card_subtype,'')::VARCHAR                AS card_subtype,
    NULLIF(card_title,'')::VARCHAR                  AS card_title,
    NULLIF(card_type,'')::VARCHAR                   AS card_type,
    NULLIF(comment_message,'')::VARCHAR             AS comment_message,
    NULLIF(comment_status,'')::VARCHAR              AS comment_status,
    NULLIF(content_status,'')::VARCHAR              AS content_status,
    NULLIF(content_structure,'')::VARCHAR           AS content_structure,
    NULLIF(country,'')::VARCHAR                     AS country,
    NULLIF(department,'')::VARCHAR                  AS department,
    NULLIF(division,'')::VARCHAR                    AS division,
    NULLIF(duration_hh_mm_,'')::VARCHAR             AS duration_hh_mm,
    NULLIF(ecl_id,'')::VARCHAR                      AS ecl_id,
    NULLIF(ecl_source_name,'')::VARCHAR             AS ecl_source_name,
    NULLIF(email,'')::VARCHAR                       AS email,
    NULLIF(event,'')::VARCHAR                       AS event,
    excluded_from_leaderboard::BOOLEAN              AS excluded_from_leaderboard,
    NULLIF(follower_user_full_name_,'')::VARCHAR    AS follower_user_full_name,
    NULLIF(following_user_full_name_,'')::VARCHAR   AS following_user_full_name,
    NULLIF(gitlab_internal,'')::BOOLEAN             AS gitlab_internal,
    NULLIF(group_name,'')::VARCHAR                  AS group_name,
    NULLIF(group_status,'')::VARCHAR                AS group_status,
    NULLIF(hire_date,'')::DATE                      AS hire_date,
    NULLIF(impartner_account,'')::VARCHAR           AS impartner_account,
    NULLIF(is_card_promoted,'')::BOOLEAN            AS is_card_promoted,
    NULLIF(is_live_stream,'')::BOOLEAN              AS is_live_stream,
    NULLIF(is_manager,'')::BOOLEAN                  AS is_manager,
    NULLIF(is_public_,'')::BOOLEAN                  AS is_public,
    NULLIF(job_groups,'')::VARCHAR                  AS job_groups,
    NULLIF(performance_metric,'')::VARCHAR          AS performance_metric,
    NULLIF(platform,'')::VARCHAR                    AS platform,
    NULLIF(region,'')::VARCHAR                      AS region,
    NULLIF(role,'')::VARCHAR                        AS role_name,
    NULLIF(shared_to_group_name,'')::VARCHAR        AS shared_to_group_name,
    NULLIF(shared_to_user_full_name,'')::VARCHAR    AS shared_to_user_full_name,
    sign_in_count::NUMBER                           AS sign_in_count,
    NULLIF(standard_card_type,'')::VARCHAR          AS standard_card_type,
    NULLIF(supervisor,'')::VARCHAR                  AS supervisor,
    NULLIF(supervisor_email,'')::VARCHAR            AS supervisor_email,
    time::TIMESTAMP                                 AS time,
    time_account_created::TIMESTAMP                 AS time_account_created,
    NULLIF(title,'')::VARCHAR                       AS title,
    NULLIF(user_account_status,'')::VARCHAR         AS user_account_status,
    NULLIF(user_full_name,'')::VARCHAR              AS user_full_name,
    __loaded_at::TIMESTAMP                          AS __loaded_at
  FROM deduplicated

)

SELECT *
FROM renamed
