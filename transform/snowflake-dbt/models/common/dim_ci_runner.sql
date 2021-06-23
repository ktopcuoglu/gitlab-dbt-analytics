WITH prep_ci_runner AS (

    SELECT 
      dim_ci_runner_id, 
      
      -- FOREIGN KEYS
      dim_ci_build_id,
      dim_ci_stage_id,
      dim_project_id,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      created_date_id,
      dim_plan_id,
      dim_user_id,
     
      created_at,
      updated_at,
      description,
      contacted_at,
      is_active,
      runner_name,
      version,
      revision,
      platform,
      architecture,
      is_untagged,
      is_locked,
      access_level,
      ip_address,
      maximum_timeout,
      runner_type,
      public_projects_minutes_cost_factor,
      private_projects_minutes_cost_factor
    
    FROM {{ ref('prep_ci_runner') }}

)

{{ dbt_audit(
    cte_ref="prep_ci_runner",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-06-23",
    updated_date="2021-06-23"
) }}

