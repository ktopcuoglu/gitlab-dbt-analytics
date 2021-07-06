WITH prep_action AS (

    SELECT 
      dim_action_id,
          
      -- FOREIGN KEYS
      dim_project_id,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      dim_user_id,
      created_date_id,
      dim_plan_id,

      -- events metadata
      target_id,
      target_type,
      created_at,
      event_action_type
    FROM {{ ref('prep_action') }}

)

{{ dbt_audit(
    cte_ref="prep_action",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-05-19",
    updated_date="2021-05-19"
) }}
