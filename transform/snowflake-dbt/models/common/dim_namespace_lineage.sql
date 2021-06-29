WITH lineage AS (

    SELECT
      dim_namespace_id,

      -- Foreign Keys & IDs
      ultimate_parent_namespace_id,
      parent_id,
      namespace_plan_id,
      ultimate_parent_plan_id,

      -- Namespace metadata
      upstream_lineage,
      is_currently_valid,

      -- GitLab subscription and plan dimensions
      namespace_plan_title,
      namespace_plan_is_paid,
      ultimate_parent_plan_title,
      ultimate_parent_plan_is_paid
    FROM {{ ref('prep_namespace_lineage') }}
    
)

{{ dbt_audit(
    cte_ref="lineage",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-06-16",
    updated_date="2021-06-16"
) }}