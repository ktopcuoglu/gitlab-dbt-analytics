WITH prep_user AS (

    SELECT 
      dim_event_id,
          
      -- FOREIGN KEYS
      dim_project_id,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      dim_user_id,
      event_creation_dim_date_id,
      dim_plan_id,

      -- events metadata
      target_id,
      target_type,
      created_at,
      event_action_type
    FROM {{ ref('prep_user') }}

)

{{ dbt_audit(
    cte_ref="prep_user",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-21",
    updated_date="2021-05-21"
) }}
