{{ config(
    tags=["product"]
) }}

WITH prep_namespace AS (

    SELECT *
    FROM {{ ref('prep_namespace') }}

), joined AS (

    SELECT
      dim_namespace_id,
      namespace_id,
      namespace_is_internal,
      namespace_is_ultimate_parent,
      namespace_name,
      namespace_path,
      owner_id,
      namespace_type,
      has_avatar,
      namespace_created_at,
      namespace_updated_at,
      is_membership_locked,
      has_request_access_enabled,
      has_share_with_group_locked,
      visibility_level,
      ldap_sync_status,
      ldap_sync_error,
      ldap_sync_last_update_at,
      ldap_sync_last_successful_update_at,
      ldap_sync_last_sync_at,
      lfs_enabled,
      parent_id,
      shared_runners_enabled, 
      shared_runners_minutes_limit,
      extra_shared_runners_minutes_limit,
      repository_size_limit,
      does_require_two_factor_authentication,
      two_factor_grace_period,
      project_creation_level,
      push_rule_id,
      creator_id,
      ultimate_parent_namespace_id,
      gitlab_plan_id,
      gitlab_plan_title,
      gitlab_plan_is_paid,
      dim_product_tier_id,
      current_member_count,
      current_project_count
    FROM prep_namespace
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@snalamaru",
    updated_by="@ischweickartDD",
    created_date="2020-12-29",
    updated_date="2021-01-14"
) }}
