{{ config(
    tags=["people", "edcast"]
) }}


WITH source AS (

  SELECT *
  FROM {{ source('edcast', 'glue_groups_g3_group_performance_data_explorer') }}

), renamed AS (

  SELECT
    assigned_content::BOOLEAN                       AS assigned_content,
    card_author_full_name::VARCHAR                  AS card_author_full_name,
    card_resource_url::VARCHAR                      AS card_resource_url,
    card_state::VARCHAR                             AS card_state,
    card_subtype::VARCHAR                           AS card_subtype,
    card_title::VARCHAR                             AS card_title,
    card_type::VARCHAR                              AS card_type,
    comment_message::VARCHAR                        AS comment_message,
    comment_status::VARCHAR                         AS comment_status,
    content_status::VARCHAR                         AS content_status,
    content_structure::VARCHAR                      AS content_structure,
    country::VARCHAR                                AS country,
    department::VARCHAR                             AS department,
    division::VARCHAR                               AS division,
    duration_hh_mm_::VARCHAR                        AS duration_hh_mm,
    ecl_id::VARCHAR                                 AS ecl_id,
    ecl_source_name::VARCHAR                        AS ecl_source_name,
    email::VARCHAR                                  AS email,
    event::VARCHAR                                  AS event,
    excluded_from_leaderboard::BOOLEAN              AS excluded_from_leaderboard,
    follower_user_full_name_::VARCHAR               AS follower_user_full_name,
    following_user_full_name_::VARCHAR              AS following_user_full_name,
    DECODE(gitlab_internal,'f',FALSE,TRUE)::BOOLEAN AS gitlab_internal,
    group_name::VARCHAR                             AS group_name,
    group_status::VARCHAR                           AS group_status,
    NULLIF(hire_date,'')::DATE                      AS hire_date,
    impartner_account::VARCHAR                      AS impartner_account,
    DECODE(is_card_promoted,0,FALSE,TRUE)::BOOLEAN  AS is_card_promoted,
    DECODE(is_live_stream,0,FALSE,TRUE)::BOOLEAN    AS is_live_stream,
    DECODE(is_manager,'f',FALSE,TRUE)::BOOLEAN      AS is_manager,
    is_public_::BOOLEAN                             AS is_public,
    job_groups::VARCHAR                             AS job_groups,
    performance_metric::VARCHAR                     AS performance_metric,
    platform::VARCHAR                               AS platform,
    region::VARCHAR                                 AS region,
    role::VARCHAR                                   AS role,
    shared_to_group_name::VARCHAR                   AS shared_to_group_name,
    shared_to_user_full_name::VARCHAR               AS shared_to_user_full_name,
    sign_in_count::NUMBER                           AS sign_in_count,
    standard_card_type::VARCHAR                     AS standard_card_type,
    supervisor::VARCHAR                             AS supervisor,
    supervisor_email::VARCHAR                       AS supervisor_email,
    time::TIMESTAMP                                 AS time,
    time_account_created::TIMESTAMP                 AS time_account_created,
    title::VARCHAR                                  AS title,
    user_account_status::VARCHAR                    AS user_account_status,
    user_full_name::VARCHAR                         AS user_full_name,
    __loaded_at::TIMESTAMP                          AS __loaded_at
  FROM source

)

SELECT *
FROM renamed
