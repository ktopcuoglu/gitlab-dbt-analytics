    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_settings_dedupe_source') }}
  
), renamed AS (

    SELECT 
      created_at::TIMESTAMP,
      updated_at::TIMESTAMP,
      namespace_id::NUMBER,
      prevent_forking_outside_group::BOOLEAN,
      allow_mfa_for_subgroups::BOOLEAN,
      default_branch_name::VARCHAR,
      repository_read_only::BOOLEAN,
      delayed_project_removal::BOOLEAN,
      resource_access_token_creation_allowed::BOOLEAN,
      lock_delayed_project_removal::BOOLEAN,
      prevent_sharing_groups_outside_hierarchy::BOOLEAN,
      new_user_signups_cap ::NUMBER,
      setup_for_company::BOOLEAN,
      jobs_to_be_done::NUMBER 
    FROM source

)

SELECT *
FROM renamed
