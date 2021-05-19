WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_onboarding_progresses_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER                                    AS onboarding_progress_id,
      namespace_id::NUMBER                          AS namespace_id,
      created_at::TIMESTAMP                         AS created_at,
      updated_at::TIMESTAMP                         AS updated_at,
      git_pull_at::TIMESTAMP                        AS git_pull_at,
      git_write_at::TIMESTAMP                       AS git_write_at,
      merge_request_created_at::TIMESTAMP           AS merge_request_created_at,
      pipeline_created_at::TIMESTAMP                AS pipeline_created_at,
      user_added_at::TIMESTAMP                      AS user_added_at,
      trial_started_at::TIMESTAMP                   AS trial_started_at,
      subscription_created_at::TIMESTAMP            AS subscription_created_at,
      required_mr_approvals_enabled_at::TIMESTAMP   AS required_mr_approvals_enabled_at,
      code_owners_enabled_at::TIMESTAMP             AS code_owners_enabled_at,
      scoped_label_created_at::TIMESTAMP            AS scoped_label_created_at,
      security_scan_enabled_at::TIMESTAMP           AS security_scan_enabled_at,
      issue_auto_closed_at::TIMESTAMP               AS issue_auto_closed_at,
      repository_imported_at::TIMESTAMP             AS repository_imported_at,
      repository_mirrored_at::TIMESTAMP             AS repository_mirrored_at
    FROM source

)

SELECT *
FROM renamed