WITH prep_ci_runner AS (

    SELECT 
      dim_ci_runner_id, 
      
      -- FOREIGN KEYS
      created_date_id,
     
      created_at,
      updated_at,
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

