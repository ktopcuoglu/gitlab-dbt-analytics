    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_settings_dedupe_source') }}
  
), renamed AS (

    SELECT 
      created_at::TIMESTAMP                               AS CREATED_AT,
      updated_at::TIMESTAMP                               AS UPDATED_AT,
      namespace_id::NUMBER                                AS NAMESPACE_ID,
      prevent_forking_outside_group::BOOLEAN              AS PREVENT_FORKING_OUTSIDE_GROUP,
      allow_mfa_for_subgroups::BOOLEAN                    AS ALLOW_MFA_FOR_SUBGROUPS,
      default_branch_name::VARCHAR                        AS DEFAULT_BRANCH_NAME,
      repository_read_only::BOOLEAN                       AS REPOSITORY_READ_ONLY,
      delayed_project_removal::BOOLEAN                    AS DELAYED_PROJECT_REMOVAL,
      resource_access_token_creation_allowed::BOOLEAN     AS RESOURCE_ACCESS_TOKEN_CREATION_ALLOWED,
      lock_delayed_project_removal::BOOLEAN               AS LOCK_DELAYED_PROJECT_REMOVAL,
      prevent_sharing_groups_outside_hierarchy::BOOLEAN   AS PREVENT_SHARING_GROUPS_OUTSIDE_HIERARCHY,
      new_user_signups_cap::NUMBER                        AS NEW_SIGNUPS_CAP,
      setup_for_company::BOOLEAN                          AS SETUP_FOR_COMPANY,
      jobs_to_be_done::NUMBER                             AS JOBS_TO_BE_DONE
    FROM source

)

SELECT *
FROM renamed
