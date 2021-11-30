WITH prep_ci_runner AS (

    SELECT 
      dim_ci_runner_id, 
      
      -- FOREIGN KEYS
      created_date_id,
     
      created_at,
      updated_at,
      ci_runner_description,
      CASE 
        WHEN ci_runner_description LIKE '%private%manager%'
          THEN 'private-runner-mgr'
        WHEN ci_runner_description LIKE 'shared-runners-manager%'
          THEN 'linux-runner-mgr'
        WHEN ci_runner_description LIKE '%.shared.runners-manager.%' 
          THEN 'linux-runner-mgr'
        WHEN ci_runner_description LIKE 'gitlab-shared-runners-manager%'
          THEN 'gitlab-internal-runner-mgr'
        WHEN ci_runner_description LIKE 'windows-shared-runners-manager%'
          THEN 'windows-runner-mgr'
        WHEN ci_runner_description LIKE '%.shared-gitlab-org.runners-manager.%' 
          THEN 'shared-gitlab-org-runner-mgr'
        ELSE 'Other'
      END                                           AS ci_runner_manager,
      contacted_at,
      is_active,
      ci_runner_version,
      revision,
      platform,
      architecture,
      is_untagged,
      is_locked,
      access_level,
      maximum_timeout,
      runner_type                                   AS ci_runner_type,
      CASE runner_type
        WHEN 1 THEN 'shared'
        WHEN 2 THEN 'group-runner-hosted runners'
        WHEN 3 THEN 'project-runner-hosted runners' 
      END                                           AS ci_runner_type_summary,
      public_projects_minutes_cost_factor,
      private_projects_minutes_cost_factor
    
    FROM {{ ref('prep_ci_runner') }}

)

{{ dbt_audit(
    cte_ref="prep_ci_runner",
    created_by="@snalamaru",
    updated_by="@davis_townsend",
    created_date="2021-06-23",
    updated_date="2021-11-09"
) }}

