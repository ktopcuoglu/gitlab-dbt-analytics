{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "mart_service_ping_instance_metric_id"
) }}

{{ simple_cte([
    ('mart_service_ping_instance_metric', 'mart_service_ping_instance_metric')
    ])

}}

, final AS (

  SELECT
      mart_service_ping_instance_metric_id,
      dim_date_id,
      metrics_path,
      metric_value,
      has_timed_out,
      dim_service_ping_instance_id,
      dim_instance_id,
      dim_license_id,
      dim_installation_id,
      latest_active_subscription_id,
      dim_billing_account_id,
      dim_parent_crm_account_id,
      major_minor_version_id,
      dim_host_id,
      host_name,
      service_ping_delivery_type,
      ping_edition,
      ping_product_tier,
      ping_edition_product_tier,
      major_version,
      minor_version,
      major_minor_version,
      version_is_prerelease,
      is_internal,
      is_staging,
      is_trial,
      umau_value,
      group_name,
      stage_name,
      section_name,
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      time_frame,
      instance_user_count,
      original_subscription_name_slugify,
      subscription_start_month,
      subscription_end_month,
      product_category_array,
      product_rate_plan_name_array,
      is_paid_subscription,
      is_program_subscription,
      crm_account_name,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      technical_account_manager,
      ping_created_at,
      ping_created_at_month,
      is_last_ping_of_month
  FROM mart_service_ping_instance_metric WHERE time_frame = 'all'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-04-08",
    updated_date="2022-04-08"
) }}
