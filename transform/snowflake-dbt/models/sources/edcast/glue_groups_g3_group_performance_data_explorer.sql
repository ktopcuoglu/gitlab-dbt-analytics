{{ config(
    tags=["people", "edcast"]
) }}


WITH source AS (

  SELECT *
  FROM {{ source('edcast', 'glue_groups_g3_group_performance_data_explorer') }}

), renamed AS (

  SELECT
    group_id                            AS group_id                        ,                   --:___9625
    group_name                          AS group_name                      ,                   --:_gitlab_learning_community
    group_status                        AS group_status                    ,                   --:_active
    user_id                             AS user_id                         ,                   --:_460679.0
    card_id                             AS card_id                         ,                   --:_7089752.0
    card_state                          AS card_state                      ,                   --_published
    card_title                          AS card_title                      ,                   --_nurturing_psychological_safety_for_all_team_members
    card_type                           AS card_type                       ,                   --_media
    is_user_generated                   AS is_user_generated               ,                   --_1
    content_structure                   AS content_structure               ,                   --_smartcard
    card_author_id                      AS card_author_id                  ,                   --_460875.0
    user_full_name                      AS user_full_name                  ,                   --_matt_kasa
    content_status                      AS content_status                  ,                   --_active
    email                               AS email                           ,                   --_mkasa@gitlab.com
    time                                AS time                            ,                   --_2021-12-14t00    --11    --39
    event                               AS event                           ,                   --_card_source_visited
    shared_to_user_id                   AS shared_to_user_id               ,                   --_
    shared_to_group_id                  AS shared_to_group_id              ,                   --_
    platform                            AS platform                        ,                   --_web
    performance_metric                  AS performance_metric              ,                   --_total_content_source_visits
    comment_id                          AS comment_id                      ,                   --_
    comment_message                     AS comment_message                 ,                   --_
    follower_id                         AS follower_id                     ,                   --_    _followed
    user_id                             AS user_id                         ,                   --_
    shared_to_user_full_name            AS shared_to_user_full_name        ,                   --_
    follower_user_full_name             AS follower_user_full_name         ,                   --_
    shared_to_group_name                AS shared_to_group_name            ,                   --_
    card_author_full_name               AS card_author_full_name           ,                   --_liam_mcnally
    comment_status                      AS comment_status                  ,                   --_
    time_account_created                AS time_account_created            ,                   --_2020-12-08t21    --10    --52
    sign-in_count                       AS sign-in_count                   ,                   --_3
    user_account_status                 AS user_account_status             ,                   --_active
    excluded_from_leaderboard           AS excluded_from_leaderboard       ,                   --_false
    card_resource_url                   AS card_resource_url               ,                   --_https    --//youtu.be/2thoaaojrtk
    is_live_stream                      AS is_live_stream                  ,                   --_0
    card_subtype                        AS card_subtype                    ,                   --_link
    ecl_id                              AS ecl_id                          ,                   --_2d3774c0-4887-44e3-8677-302bbbbf55c9
    ecl_source_name                     AS ecl_source_name                 ,                   --_user_generated_content
    is_card_promoted                    AS is_card_promoted                ,                   --_0    _
    is_public                           AS is_public                       ,                   --_true    _
    duration_seconds                    AS duration_seconds                ,                   --_0    _
    duration_hh                         AS duration_hh                     ,                   --mm    --_00    --00    _
    assigned_content                    AS assigned_content                ,                   --_false    _
    title                               AS title                           ,                   --_senior_backend_engineer    _
    supervisor_email                    AS supervisor_email                ,                   --_nklick@gitlab.com    _
    supervisor                          AS supervisor                      ,                   --_nicholas_klick    _
    job_groups                          AS job_groups                      ,                   --_    _
    role                                AS role                            ,                   --_individual_contributor
    country                             AS country                         ,                   --_united_states    _
    hire_date                           AS hire_date                       ,                   --_2019-07-22    _
    department                          AS department                      ,                   --_development    _
    division                            AS division                        ,                   --: "Sales"
    group_id                            AS group_id                        ,                   --:___9625
    group_name                          AS group_name                      ,                   --:_gitlab_learning_community
    group_status                        AS group_status                    ,                   --:_active
    user_id                             AS user_id                         ,                   --:_460679.0
    card_id                             AS card_id                         ,                   --:_7089752.0
    card_state                          AS card_state                      ,                   --_published
    card_title                          AS card_title                      ,                   --_nurturing_psychological_safety_for_all_team_members
    card_type                           AS card_type                       ,                   --_media
    is_user_generated                   AS is_user_generated               ,                   --_1
    content_structure                   AS content_structure               ,                   --_smartcard
    card_author_id                      AS card_author_id                  ,                   --_460875.0
    user_full_name                      AS user_full_name                  ,                   --_matt_kasa
    content_status                      AS content_status                  ,                   --_active
    email                               AS email                           ,                   --_mkasa@gitlab.com
    time                                AS time                            ,                   --_2021-12-14t00    --11    --39
    event                               AS event                           ,                   --_card_source_visited
    shared_to_user_id                   AS shared_to_user_id               ,                   --_
    shared_to_group_id                  AS shared_to_group_id              ,                   --_
    platform                            AS platform                        ,                   --_web
    performance_metric                  AS performance_metric              ,                   --_total_content_source_visits
    comment_id                          AS comment_id                      ,                   --_
    comment_message                     AS comment_message                 ,                   --_
    follower_id                         AS follower_id                     ,                   --_    _followed
    user_id                             AS user_id                         ,                   --_
    shared_to_user_full_name            AS shared_to_user_full_name        ,                   --_
    follower_user_full_name             AS follower_user_full_name         ,                   --_
    shared_to_group_name                AS shared_to_group_name            ,                   --_
    card_author_full_name               AS card_author_full_name           ,                   --_liam_mcnally
    comment_status                      AS comment_status                  ,                   --_
    time_account_created                AS time_account_created            ,                   --_2020-12-08t21    --10    --52
    sign-in_count                       AS sign-in_count                   ,                   --_3
    user_account_status                 AS user_account_status             ,                   --_active
    excluded_from_leaderboard           AS excluded_from_leaderboard       ,                   --_false
    card_resource_url                   AS card_resource_url               ,                   --_https    --//youtu.be/2thoaaojrtk
    is_live_stream                      AS is_live_stream                  ,                   --_0
    card_subtype                        AS card_subtype                    ,                   --_link
    ecl_id                              AS ecl_id                          ,                   --_2d3774c0-4887-44e3-8677-302bbbbf55c9
    ecl_source_name                     AS ecl_source_name                 ,                   --_user_generated_content
    is_card_promoted                    AS is_card_promoted                ,                   --_0    _
    is_public                           AS is_public                       ,                   --_true    _
    duration_seconds                    AS duration_seconds                ,                   --_0    _
    duration_hh                         AS duration_hh                     ,                   --mm    --_00    --00    _
    assigned_content                    AS assigned_content                ,                   --_false    _
    title                               AS title                           ,                   --_senior_backend_engineer    _
    supervisor_email                    AS supervisor_email                ,                   --_nklick@gitlab.com    _
    supervisor                          AS supervisor                      ,                   --_nicholas_klick    _
    job_groups                          AS job_groups                      ,                   --_    _
    role                                AS role                            ,                   --_individual_contributor
    country                             AS country                         ,                   --_united_states    _
    hire_date                           AS hire_date                       ,                   --_2019-07-22    _
    department                          AS department                      ,                   --_development    _
    division                            AS division                        ,                   --_engineering    _
    region                              AS region                          ,                   --_americas    _
    is_manager                          AS is_manager                      ,                   --_f    _
    gitlab_internal                     AS gitlab_internal                 ,                   --_t    _
    impartner_account                   AS impartner_account               ,                   --_    _
    standard_card_type                  AS standard_card_type              ,                --_engineering    _
    region                              AS region                          ,                   --_americas    _
    is_manager                          AS is_manager                      ,                   --_f    _
    gitlab_internal                     AS gitlab_internal                 ,                   --_t    _
    impartner_account                   AS impartner_account               ,                   --_    _
    standard_card_type                  AS standard_card_type,
  FROM source

)

SELECT *
FROM renamed
